/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.*;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.utils.Throwables.merge;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Blocker;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;

/**
 * IMPORTANT: When this object is involved in a transactional graph, and is not encapsulated in a LifecycleTransaction,
 * for correct behaviour its commit MUST occur before any others, since it may legitimately fail. This is consistent
 * with the Transactional API, which permits one failing action to occur at the beginning of the commit phase, but also
 * *requires* that the prepareToCommit() phase only take actions that can be rolled back.
 *
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept; vice-versa if it fails.
 *
 * The transaction log file contains new and old sstables as follows:
 *
 * add: sstable-2
 * remove: sstable-1
 *
 * where sstable-2 is a new sstable to be retained if the transaction succeeds and sstable-1 is an old sstable to be
 * removed. Upon commit we add a final line to the log file:
 *
 * commit
 *
 * When the transaction log is cleaned-up by the TransactionTidier, which happens only after any old sstables have been
 * osoleted, then any sstable files for old sstables are removed before deleting the transaction log if the transaction
 * was committed, vice-versa if the transaction was aborted.
 *
 * On start-up we look for any transaction log files and repeat the cleanup process described above.
 *
 * See CASSANDRA-7066 for full details.
 */
public class TransactionLog extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionLog.class);

    /**
     * If the format of the lines in the transaction log is wrong or the CRC
     * checks fail, then we throw this exception.
     */
    public static final class CorruptTransactionLogException extends RuntimeException
    {
        public final TransactionFile file;

        public CorruptTransactionLogException(String message, TransactionFile file)
        {
            super(message);
            this.file = file;
        }
    }

    /**
     * The type of record to be logged, new files, old files or the commit flag.
     */
    public enum RecordType
    {
        NEW ("add"),
        OLD ("remove"),
        COMMIT("commit");

        public final String prefix;

        RecordType(String prefix)
        {
            this.prefix = prefix;
        }

        public static RecordType fromPrefix(String prefix)
        {
            for (RecordType t : RecordType.values())
            {
                if (t.prefix.equals(prefix))
                    return t;
            }

            throw new IllegalStateException("Invalid prefix : " + prefix);
        }
    }

    final static class Record
    {
        public final RecordType type;
        public final String filePath;
        public final long updateTime;
        public final long checksum;
        public final String record;

        static String REGEX_STR = "^(add|remove|commit):\\[([^,]*),?([^,]*),?([^,]*)?\\]$";
        static Pattern REGEX = Pattern.compile(REGEX_STR); // (add|remove|commit):[*,*,*]

        public static Record make(String record)
        {
            Matcher matcher = REGEX.matcher(record);
            if (!matcher.matches() || matcher.groupCount() != 4)
                throw new IllegalStateException(String.format("Invalid record \"%s\"", record));

            RecordType type = RecordType.fromPrefix(matcher.group(1));
            switch (type)
            {
                case COMMIT:
                    return makeCommit(Long.valueOf(matcher.group(2)), record);
                case NEW:
                    return makeNew(matcher.group(2), record);
                case OLD:
                    return makeOld(matcher.group(2), Long.valueOf(matcher.group(3)), Long.valueOf(matcher.group(4)), record);
                default:
                    throw new AssertionError("Invalid record type : " + type);
            }
        }

        public static Record makeCommit(long updateTime)
        {
            return makeCommit(updateTime, "");
        }

        private static Record makeCommit(long updateTime, String record)
        {
            return new Record(RecordType.COMMIT, "", updateTime, 0, record);
        }

        public static Record makeNew(String filePath)
        {
            return makeNew(filePath, "");
        }

        private static Record makeNew(String filePath, String record)
        {
            return new Record(RecordType.NEW, filePath, 0, 0, record);
        }

        public static Record makeOld(String filePath, List<File> files)
        {
            long lastModified = 0;
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
            CRC32 checksum = new CRC32();
            for (File file : files)
            {
                if (file.lastModified() > lastModified)
                    lastModified = file.lastModified();

                buffer.clear();
                buffer.putLong(file.lastModified());
                buffer.putLong(file.length());

                buffer.flip();
                checksum.update(buffer);
            }
            return Record.makeOld(filePath, lastModified, checksum.getValue(), "");
        }

        private static Record makeOld(String filePath, long updateTime, long checksum, String record)
        {
            return new Record(RecordType.OLD, filePath, updateTime, checksum, record);
        }

        private Record(RecordType type,
                       String filePath,
                       long updateTime,
                       long checksum,
                       String record)
        {
            this.type = type;
            this.filePath = filePath;
            this.updateTime = updateTime;
            this.checksum = checksum;
            this.record = record.isEmpty() ? format() : record;
        }

        private String format()
        {
            switch(type)
            {
                case COMMIT:
                    return String.format("%s:[%d]", type.prefix, updateTime);
                case NEW:
                    return String.format("%s:[%s]", type.prefix, filePath);
                case OLD:
                    return String.format("%s:[%s,%d,%d]", type.prefix, filePath, updateTime, checksum);
                default:
                    throw new AssertionError("Invalid record type : " + type);
            }
        }

        public byte[] getBytes()
        {
            return record.getBytes();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, filePath);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
                return false;

            if (getClass() != obj.getClass())
                return false;

            final Record other = (Record)obj;

            // we exclude on purpose checksum and update time as
            // we don't want duplicated records that differ only by
            // checsum or update time
            return type.equals(other.type) &&
                   filePath.equals(other.filePath);
        }

        @Override
        public String toString()
        {
            return record;
        }
    }

    /**
     * The transaction log file.
     */
    final static class TransactionFile
    {
        static String EXT = ".log";
        static String COMMIT = "commit:";
        static char SEP = '_';
        static String FILE_REGEX_STR = String.format("^(.*)_(.*)%s$", EXT);
        static Pattern FILE_REGEX = Pattern.compile(FILE_REGEX_STR); //(opname)_(id).log
        static String LINE_REGEX_STR = "^(.*)\\[(\\d*)\\]$";
        static Pattern LINE_REGEX = Pattern.compile(LINE_REGEX_STR); //*[checksum]

        public final File file;
        public final TransactionData parent;
        public final Set<Record> records = new HashSet<>();
        public final Checksum checksum = new CRC32();

        public TransactionFile(TransactionData parent)
        {
            this.file = new File(parent.getFileName());
            this.parent = parent;
        }

        public void readRecords()
        {
            records.clear();
            checksum.reset();

            FileUtils.readLines(file).forEach(line -> records.add(readRecord(line)));
        }

        private Record readRecord(String line)
        {
            Matcher matcher = LINE_REGEX.matcher(line);
            if (!matcher.matches() || matcher.groupCount() != 2)
                throw new CorruptTransactionLogException(String.format("Failed to parse transaction line \"%s\"", line), this);

            byte[] bytes = matcher.group(1).getBytes();
            checksum.update(bytes, 0, bytes.length);

            if (checksum.getValue() != Long.valueOf(matcher.group(2)))
                throw new CorruptTransactionLogException(String.format("Invalid checksum %s for line \"%s\", expected %d",
                                                                       matcher.group(2),
                                                                       line,
                                                                       checksum.getValue()),
                                                         this);

            try
            {
                return Record.make(matcher.group(1));
            }
            catch (Throwable t)
            {
                throw new CorruptTransactionLogException(String.format("Failed to parse record for line \"%s\": %s", line, t.getMessage()), this);
            }
        }

        public void commit()
        {
            Record record = Record.makeCommit(System.currentTimeMillis());
            assert !records.contains(record) : "Already committed!";

            addRecord(record);
        }

        public boolean committed()
        {
            return records.contains(Record.makeCommit(System.currentTimeMillis()));
        }

        public boolean add(RecordType type, SSTable table)
        {
            Record record = makeRecord(type, table);
            if (records.contains(record))
                return false;

            addRecord(record);
            return true;
        }

        private Record makeRecord(RecordType type, SSTable table)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), table.descriptor.baseFilename());
            if (type == RecordType.NEW)
            {
                return Record.makeNew(relativePath);
            }
            else if (type == RecordType.OLD)
            {
                return Record.makeOld(relativePath, getTableFiles(relativePath));
            }
            else
            {
                throw new AssertionError("Invalid record type " + type);
            }
        }

        private List<File> getTableFiles(final String relativePath)
        {
            return Arrays.asList(new File(parent.getParentFolder()).listFiles((dir, name) -> {
                return name.startsWith(relativePath);
            }));
        }

        private void addRecord(Record record)
        {
            byte[] bytes = record.getBytes();
            checksum.update(bytes, 0, bytes.length);

            records.add(record);
            FileUtils.append(file, String.format("%s[%d]", record, checksum.getValue()));

            parent.sync();
        }

        public void remove(RecordType type, SSTable table)
        {
            Record record = makeRecord(type, table);

            assert records.contains(record) : String.format("[%s] is not tracked by %s", record, file);

            records.remove(record);
            delete(record);
        }

        public boolean contains(RecordType type, SSTable table)
        {
            return records.contains(makeRecord(type, table));
        }

        public void deleteContents(RecordType type)
        {
            assert file.exists() : String.format("Expected %s to exists", file);
            records.forEach(record ->
                            {
                                if (record.type == type)
                                    delete(record);
                            }
            );
            records.clear();
        }

        public void delete()
        {
            TransactionLog.delete(file);
        }

        private void delete(Record record)
        {
            List<File> files = getTrackedFiles(record);
            if (files.isEmpty())
                return; // Files no longer exist, nothing to do

            if (record.type == RecordType.OLD)
            {
                // Paranoid sanity checks: we create another record by looking at files as they are
                // on disk right now, if either the last update time or the checksum do not match
                // what we save when the table was obsoleted, it means something went horribly wrong:
                // either the file system is corrupt or the user has overwrittent the files, so we
                // log a panic and skip deleting this record files
                Record currentRecord = Record.makeOld(record.filePath, files);
                if (record.updateTime != currentRecord.updateTime)
                {
                    logger.error("PANIC!!! - Did not delete obsoleted files for sstable {}, last update time mismatch: {} should have been {}",
                                 record.filePath,
                                 currentRecord.updateTime,
                                 record.updateTime);
                    return;
                }

                if (record.checksum != currentRecord.checksum)
                {
                    logger.error("PANIC!!! - Did not delete obsoleted files for sstable {}, checksum mismatch: {} should have been {}",
                                 record.filePath,
                                 currentRecord.checksum,
                                 record.checksum);
                    return;
                }

                // sort them in ascending order of last modified so that they are deleted from olest to newest
                // benedit: why is this necessary?
                files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));
            }

            files.forEach(file -> TransactionLog.delete(file));
        }

        public Set<File> getTrackedFiles(RecordType type)
        {
            Set<File> ret = new HashSet<>();
            records.forEach(record ->
                            {
                                if (record.type == type)
                                    ret.addAll(getTrackedFiles(record));
                            });
            ret.add(file);
            return ret;
        }

        private List<File> getTrackedFiles(Record record)
        {
            if (record.type.equals(COMMIT))
                return Collections.emptyList();

            return Arrays.asList(new File(parent.getParentFolder()).listFiles((dir, name) -> {
                return name.startsWith(record.filePath);
            }));
        }

        public boolean exists()
        {
            return file.exists();
        }
    }

    /**
     * We split the transaction data from the behavior because we need
     * to reconstruct any left-overs and clean them up, as well as work
     * out which files are temporary. So for these cases we don't want the full
     * transactional behavior, plus it's handy for the TransactionTidier.
     */
    final static class TransactionData implements AutoCloseable
    {
        private final OperationType opType;
        private final UUID id;
        private final File folder;
        private final TransactionFile file;
        private int folderDescriptor;

        static TransactionData make(File logFile)
        {
            Matcher matcher = TransactionFile.FILE_REGEX.matcher(logFile.getName());
            assert matcher.matches();

            OperationType operationType = OperationType.fromFileName(matcher.group(1));
            UUID id = UUID.fromString(matcher.group(2));

            return new TransactionData(operationType, logFile.getParentFile(), id);
        }

        TransactionData(OperationType opType, File folder, UUID id)
        {
            this.opType = opType;
            this.id = id;
            this.folder = folder;
            this.file = new TransactionFile(this);
            this.folderDescriptor = CLibrary.tryOpenDirectory(folder.getPath());
        }

        public void readLogFile()
        {
            file.readRecords();
        }

        public void close()
        {
            if (folderDescriptor > 0)
            {
                CLibrary.tryCloseFD(folderDescriptor);
                folderDescriptor = -1;
            }
        }

        void sync()
        {
            if (folderDescriptor > 0)
                CLibrary.trySync(folderDescriptor);
        }

        OperationType getType()
        {
            return opType;
        }

        UUID getId()
        {
            return id;
        }

        Throwable removeUnfinishedLeftovers(Throwable accumulate)
        {
            try
            {
                if (file.committed())
                    file.deleteContents(RecordType.OLD);
                else
                    file.deleteContents(RecordType.NEW);

                // we sync the parent file descriptor between contents and log deletion
                // to ensure there is a happens before edge between them
                sync();

                file.delete();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        Set<File> getTemporaryFiles()
        {
            sync();

            if (!file.exists())
                return Collections.emptySet();

            if (file.committed())
                return file.getTrackedFiles(RecordType.OLD);
            else
                return file.getTrackedFiles(RecordType.NEW);
        }

        String getFileName()
        {
            String fileName = StringUtils.join(opType.fileName,
                                               TransactionFile.SEP,
                                               id.toString(),
                                               TransactionFile.EXT);
            return StringUtils.join(folder, File.separator, fileName);
        }

        String getParentFolder()
        {
            return folder.getParent();
        }

        static boolean isLogFile(String name)
        {
            return TransactionFile.FILE_REGEX.matcher(name).matches();
        }

        @VisibleForTesting
        TransactionFile getLogFile()
        {
            return file;
        }
    }

    private final Tracker tracker;
    private final TransactionData data;
    private final Ref<TransactionLog> selfRef;
    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue<>();
    private static final Blocker blocker = new Blocker();

    TransactionLog(OperationType opType, CFMetaData metadata)
    {
        this(opType, metadata, null);
    }

    TransactionLog(OperationType opType, CFMetaData metadata, Tracker tracker)
    {
        this(opType, new Directories(metadata), tracker);
    }

    TransactionLog(OperationType opType, Directories directories, Tracker tracker)
    {
        this(opType, directories.getDirectoryForNewSSTables(), tracker);
    }

    TransactionLog(OperationType opType, File folder, Tracker tracker)
    {
        this.tracker = tracker;
        this.data = new TransactionData(opType,
                                        Directories.getTransactionsDirectory(folder),
                                        UUIDGen.getTimeUUID());
        this.selfRef = new Ref<>(this, new TransactionTidier(data));

        if (logger.isDebugEnabled())
            logger.debug("Created transaction logs with id {}", data.id);
    }

    /**
     * Track a reader as new.
     **/
    void trackNew(SSTable table)
    {
        if (!data.file.add(RecordType.NEW, table))
            throw new IllegalStateException(table + " is already tracked as new");
    }

    /**
     * Stop tracking a reader as new.
     */
    void untrackNew(SSTable table)
    {
        data.file.remove(RecordType.NEW, table);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced and the transaction
     * has been committed.
     */
    SSTableTidier obsoleted(SSTableReader reader)
    {
        if (data.file.contains(RecordType.NEW, reader))
        {
            if (data.file.contains(RecordType.OLD, reader))
                throw new IllegalArgumentException();

            return new SSTableTidier(reader, true, this);
        }

        if (!data.file.add(RecordType.OLD, reader))
            throw new IllegalStateException();

        if (tracker != null)
            tracker.notifyDeleting(reader);

        return new SSTableTidier(reader, false, this);
    }

    OperationType getType()
    {
        return data.getType();
    }

    UUID getId()
    {
        return data.getId();
    }

    @VisibleForTesting
    String getDataFolder()
    {
        return data.getParentFolder();
    }

    @VisibleForTesting
    String getLogsFolder()
    {
        return StringUtils.join(getDataFolder(), File.separator, Directories.TRANSACTIONS_SUBDIR);
    }

    @VisibleForTesting
    TransactionData getData()
    {
        return data;
    }

    private static void delete(File file)
    {
        try
        {
            if (logger.isDebugEnabled())
                logger.debug("Deleting {}", file);

            Files.delete(file.toPath());
        }
        catch (NoSuchFileException e)
        {
            logger.error("Unable to delete {} as it does not exist", file);
        }
        catch (IOException e)
        {
            logger.error("Unable to delete {}", file, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * The transaction tidier.
     *
     * When the transaction reference is fully released we try to delete all the obsolete files
     * depending on the transaction result.
     */
    private static class TransactionTidier implements RefCounted.Tidy, Runnable
    {
        private final TransactionData data;

        public TransactionTidier(TransactionData data)
        {
            this.data = data;
        }

        public void tidy() throws Exception
        {
            run();
        }

        public String name()
        {
            return data.id.toString();
        }

        public void run()
        {
            if (logger.isDebugEnabled())
                logger.debug("Removing files for transaction {}", name());

            Throwable err = data.removeUnfinishedLeftovers(null);

            if (err != null)
            {
                logger.info("Failed deleting files for transaction {}, we'll retry after GC and on on server restart", name(), err);
                failedDeletions.add(this);
            }
            else
            {
                if (logger.isDebugEnabled())
                    logger.debug("Closing file transaction {}", name());
                data.close();
            }
        }
    }

    static class Obsoletion
    {
        final SSTableReader reader;
        final SSTableTidier tidier;

        public Obsoletion(SSTableReader reader, SSTableTidier tidier)
        {
            this.reader = reader;
            this.tidier = tidier;
        }
    }

    /**
     * The SSTableReader tidier. When a reader is fully released and no longer referenced
     * by any one, we run this. It keeps a reference to the parent transaction and releases
     * it when done, so that the final transaction cleanup can run when all obsolete readers
     * are released.
     */
    public static class SSTableTidier implements Runnable
    {
        // must not retain a reference to the SSTableReader, else leak detection cannot kick in
        private final Descriptor desc;
        private final long sizeOnDisk;
        private final Tracker tracker;
        private final boolean wasNew;
        private final Ref<TransactionLog> parentRef;

        public SSTableTidier(SSTableReader referent, boolean wasNew, TransactionLog parent)
        {
            this.desc = referent.descriptor;
            this.sizeOnDisk = referent.bytesOnDisk();
            this.tracker = parent.tracker;
            this.wasNew = wasNew;
            this.parentRef = parent.selfRef.tryRef();
        }

        public void run()
        {
            blocker.ask();

            SystemKeyspace.clearSSTableReadMeter(desc.ksname, desc.cfname, desc.generation);

            try
            {
                // If we can't successfully delete the DATA component, set the task to be retried later: see TransactionTidier
                File datafile = new File(desc.filenameFor(Component.DATA));

                delete(datafile);
                // let the remainder be cleaned up by delete
                SSTable.delete(desc, SSTable.discoverComponentsFor(desc));
            }
            catch (Throwable t)
            {
                logger.error("Failed deletion for {}, we'll retry after GC and on server restart", desc);
                failedDeletions.add(this);
                return;
            }

            if (tracker != null && tracker.cfstore != null && !wasNew)
                tracker.cfstore.metric.totalDiskSpaceUsed.dec(sizeOnDisk);

            // release the referent to the parent so that the all transaction files can be released
            parentRef.release();
        }

        public void abort()
        {
            parentRef.release();
        }
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedDeletions()
    {
        Runnable task;
        while ( null != (task = failedDeletions.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);
    }

    /**
     * Deletions run on the nonPeriodicTasks executor, (both failedDeletions or global tidiers in SSTableReader)
     * so by scheduling a new empty task and waiting for it we ensure any prior deletion has completed.
     */
    public static void waitForDeletions()
    {
        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(() -> {
        }, 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    public static void pauseDeletions(boolean stop)
    {
        blocker.block(stop);
    }

    private Throwable complete(Throwable accumulate)
    {
        try
        {
            accumulate = selfRef.ensureReleased(accumulate);
            return accumulate;
        }
        catch (Throwable t)
        {
            logger.error("Failed to complete file transaction {}", getId(), t);
            return Throwables.merge(accumulate, t);
        }
    }

    protected Throwable doCommit(Throwable accumulate)
    {
        data.file.commit();
        return complete(accumulate);
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        return complete(accumulate);
    }

    protected void doPrepare() { }

    /**
     * Called on startup to scan existing folders for any unfinished leftovers of
     * operations that were ongoing when the process exited.
     *
     * We check if the new transaction file exists first, and if so we clean it up
     * along with its contents, which includes the old file, else if only the old file exists
     * it means the operation has completed and we only cleanup the old file with its contents.
     */
    static void removeUnfinishedLeftovers(CFMetaData metadata)
    {
        Throwable accumulate = null;

        for (File dir : getFolders(metadata, null))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try (TransactionData data = TransactionData.make(log))
                {
                    data.readLogFile();
                    accumulate = data.removeUnfinishedLeftovers(accumulate);
                }
                catch (CorruptTransactionLogException ex)
                {
                    handleCorruptFile(ex);
                }
            }
        }

        if (accumulate != null)
            logger.error("Failed to remove unfinished transaction leftovers", accumulate);
    }

    /**
     * Return a set of files that are temporary, that is they are involved with
     * a transaction that hasn't completed yet.
     *
     * Only return the files that exist and that are located in the folder
     * specified as a parameter or its sub-folders.
     */
    static Set<File> getTemporaryFiles(CFMetaData metadata, File folder)
    {
        Set<File> ret = new HashSet<>();

        for (File dir : getFolders(metadata, folder))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try(TransactionData data = TransactionData.make(log))
                {
                    data.readLogFile();
                    ret.addAll(data.getTemporaryFiles()
                                   .stream()
                                   .filter(file -> FileUtils.isContained(folder, file))
                                   .collect(Collectors.toSet()));
                }
                catch(CorruptTransactionLogException ex)
                {
                    handleCorruptFile(ex);
                }
            }
        }

        return ret;
    }

    private static void handleCorruptFile(CorruptTransactionLogException ex)
    {
        logger.error("PANIC!!! - Failed to read corrupt transaction log {}, lines parsed so far {}", ex.file.file, ex.file.records, ex);
    }

    /**
     * Return the transaction log files that currently exist for this table.
     */
    static Set<File> getLogFiles(CFMetaData metadata)
    {
        Set<File> ret = new HashSet<>();
        for (File dir : getFolders(metadata, null))
            ret.addAll(Arrays.asList(dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            })));

        return ret;
    }

    /**
     * A utility method to work out the existing transaction sub-folders
     * either for a table, or a specific parent folder, or both.
     */
    private static List<File> getFolders(CFMetaData metadata, File folder)
    {
        List<File> ret = new ArrayList<>();
        if (metadata != null)
        {
            Directories directories = new Directories(metadata);
            ret.addAll(directories.getExistingDirectories(Directories.TRANSACTIONS_SUBDIR));
        }

        if (folder != null)
        {
            File opDir = Directories.getExistingDirectory(folder, Directories.TRANSACTIONS_SUBDIR);
            if (opDir != null)
                ret.add(opDir);
        }

        return ret;
    }
}
