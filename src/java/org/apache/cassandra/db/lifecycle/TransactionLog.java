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
 * add:[sstable-2][CRC]
 * remove:[sstable-1,max_update_time,file_crc][CRC]
 *
 * where sstable-2 is a new sstable to be retained if the transaction succeeds and sstable-1 is an old sstable to be
 * removed. CRC is an incremental CRC of the file content up to this point. For old sstable files we also log the
 * last update time of all files for the sstable descriptor and a checksum of vital properties such as update times
 * and file sizes.
 *
 * Upon commit we add a final line to the log file:
 *
 * commit:[commit_time][CRC]
 *
 * When the transaction log is cleaned-up by the TransactionTidier, which happens only after any old sstables have been
 * osoleted, then any sstable files for old sstables are removed before deleting the transaction log if the transaction
 * was committed, vice-versa if the transaction was aborted.
 *
 * On start-up we look for any transaction log files and repeat the cleanup process described above.
 *
 * See CASSANDRA-7066 for full details.
 */
public class

TransactionLog extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionLog.class);

    /**
     * If the format of the lines in the transaction log is wrong or the checksum
     * does not match, then we throw this exception.
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
     * The type of record to be logged:
     * - new files to be retained on commit
     * - old files to be retained on abort
     * - commit flag
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

            return  null;
        }
    }

    /**
     * A log file record, each record is encoded in one line and has different
     * content depending on the record type.
     */
    final static class Record
    {
        public final RecordType type;
        public final String filePath;
        public final long updateTime;
        public final int numFiles;
        public final String record;

        static String REGEX_STR = "^(add|remove|commit):\\[([^,]*),?([^,]*),?([^,]*)\\]$";
        static Pattern REGEX = Pattern.compile(REGEX_STR); // (add|remove|commit):[*,*,*]

        public static Record make(String record, boolean isLast)
        {
            try
            {
                Matcher matcher = REGEX.matcher(record);
                if (!matcher.matches() || matcher.groupCount() != 4)
                    throw new IllegalStateException(String.format("Invalid record \"%s\"", record));

                RecordType type = RecordType.fromPrefix(matcher.group(1));
                if (type == null)
                    throw new IllegalStateException("Invalid record type : " + matcher.group(1));

                return new Record(type, matcher.group(2), Long.valueOf(matcher.group(3)), Integer.valueOf(matcher.group(4)), record);
            }
            catch (Throwable t)
            {
                if (!isLast)
                    throw t;

                int pos = record.indexOf(':');
                if (pos <= 0)
                    throw t;

                RecordType recordType = RecordType.fromPrefix(record.substring(0, pos));
                if (recordType == null)
                    throw t;

                return new Record(recordType, "", 0, 0, record);

            }
        }

        public static Record makeCommit(long updateTime)
        {
            return new Record(RecordType.COMMIT, "", updateTime, 0, "");
        }

        public static Record makeNew(String filePath)
        {
            return new Record(RecordType.NEW, filePath, 0, 0, "");
        }

        public static Record makeOld(String parentFolder, String filePath)
        {
            return makeOld(getTrackedFiles(parentFolder, filePath), filePath);
        }

        public static Record makeOld(List<File> files, String filePath)
        {
            long lastModified = 0;
            for (File file : files)
            {
                if (file.lastModified() > lastModified)
                    lastModified = file.lastModified();
            }
            return new Record(RecordType.OLD, filePath, lastModified, files.size(), "");
        }

        private Record(RecordType type,
                       String filePath,
                       long updateTime,
                       int numFiles,
                       String record)
        {
            this.type = type;
            this.filePath = type == RecordType.COMMIT ? "" : filePath; // only meaningful for file records
            this.updateTime = type == RecordType.OLD ? updateTime : 0; // only meaningful for old records
            this.numFiles = type == RecordType.OLD ? numFiles : 0; // only meaningful for old records
            this.record = record.isEmpty() ? format() : record;
        }

        private String format()
        {
            return String.format("%s:[%s,%d,%d]", type.prefix, filePath, updateTime, numFiles);
        }

        public byte[] getBytes()
        {
            return record.getBytes();
        }

        public boolean verify(String parentFolder, boolean lastRecordIsCorrupt)
        {
            if (type != RecordType.OLD)
                return true;

            List<File> files = getTrackedFiles(parentFolder);

            // Paranoid sanity checks: we create another record by looking at the files as they are
            // on disk right now and make sure the information still matches
            Record currentRecord = Record.makeOld(files, filePath);
            if (updateTime != currentRecord.updateTime)
            {
                logger.error("Possible disk corruption detected for sstable [{}], record [{}]: last update time [{}] should have been [{}]",
                             filePath,
                             record,
                             new Date(currentRecord.updateTime),
                             new Date(updateTime));
                return false;
            }

            if (lastRecordIsCorrupt && currentRecord.numFiles < numFiles)
            { // if we found a corruption in the last record, then we continue only if the number of files matches exactly.
                logger.error("Possible disk corruption detected for sstable [{}], record [{}]: number of files [{}] should have been [{}]",
                             filePath,
                             record,
                             currentRecord.numFiles,
                             numFiles);
                return false;
            }

            return true;
        }

        public List<File> getTrackedFiles(String parentFolder)
        {
            if (type.equals(RecordType.COMMIT))
                return Collections.emptyList();

            return getTrackedFiles(parentFolder, filePath);
        }

        public static List<File> getTrackedFiles(String parentFolder, String filePath)
        {
            return Arrays.asList(new File(parentFolder).listFiles((dir, name) -> {
                return name.startsWith(filePath);
            }));
        }

        @Override
        public int hashCode()
        {
            // see comment in equals
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

            // we exclude on purpose checksum, update time and count as
            // we don't want duplicated records that differ only by
            // properties that might change on disk, especially COMMIT records,
            // there should be only one regardless of update time
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
     * The transaction log file, which contains many records.
     */
    final static class TransactionFile
    {
        static String EXT = ".log";
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

            Iterator<String> it = FileUtils.readLines(file).iterator();
            while(it.hasNext())
                records.add(readRecord(it.next(), !it.hasNext()));

            for (Record record : records)
            {
                if (!record.verify(parent.getParentFolder(), false))
                    throw new CorruptTransactionLogException(String.format("Failed to verify transaction %s record [%s]: possible disk corruption, aborting", parent.getId(), record),
                                                             this);
            }
        }

        private Record readRecord(String line, boolean isLast)
        {
            Matcher matcher = LINE_REGEX.matcher(line);
            if (!matcher.matches() || matcher.groupCount() != 2)
            {
                handleReadRecordError(String.format("cannot parse line \"%s\"", line), isLast);
                return Record.make(line, isLast);
            }

            byte[] bytes = matcher.group(1).getBytes();
            checksum.update(bytes, 0, bytes.length);

            if (checksum.getValue() != Long.valueOf(matcher.group(2)))
                handleReadRecordError(String.format("invalid line checksum %s for \"%s\"", matcher.group(2), line), isLast);

            try
            {
                return Record.make(matcher.group(1), isLast);
            }
            catch (Throwable t)
            {
                throw new CorruptTransactionLogException(String.format("Cannot make record \"%s\": %s", line, t.getMessage()), this);
            }
        }

        private void handleReadRecordError(String message, boolean isLast)
        {
            if (isLast)
            {
                for (Record record : records)
                {
                    if (!record.verify(parent.getParentFolder(), true))
                        throw new CorruptTransactionLogException(String.format("Last record of transaction %s is corrupt [%s] and at least " +
                                                                               "one previous record does not match state on disk, possible disk corruption, aborting",
                                                                               parent.getId(), message),
                                                                 this);
                }

                // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
                // then we simply exited whilst serializing the last record and we carry on
                logger.error(String.format("Last record of transaction %s is corrupt [%s] but all previous records match state on disk, continuing", parent.getId(), message));

            }
            else
            {
                throw new CorruptTransactionLogException(String.format("Non-last record of transaction %s is corrupt [%s], possible disk corruption, aborting", parent.getId(), message), this);
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
                return Record.makeOld(parent.getParentFolder(), relativePath);
            }
            else
            {
                throw new AssertionError("Invalid record type " + type);
            }
        }

        private void addRecord(Record record)
        {
            // benedict: the checksum is not strictly speaking on the entire file content
            // but only on the records, i.e. the checksums themseves are excluded, this
            // should be OK IMO but just to bring it to your attention
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
            deleteRecord(record);
        }

        public boolean contains(RecordType type, SSTable table)
        {
            return records.contains(makeRecord(type, table));
        }

        public void deleteRecords(RecordType type)
        {
            assert file.exists() : String.format("Expected %s to exists", file);
            records.forEach(record ->
                            {
                                if (record.type == type)
                                    deleteRecord(record);
                            }
            );
            records.clear();
        }

        private void deleteRecord(Record record)
        {
            List<File> files = record.getTrackedFiles(parent.getParentFolder());
            if (files.isEmpty())
                return; // Files no longer exist, nothing to do

            // we sort the files in ascending update time order so that the last update time
            // stays the same even if we only partially delete files
            files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

            files.forEach(file -> TransactionLog.delete(file));
        }

        public Set<File> getTrackedFiles(RecordType type)
        {
            Set<File> ret = new HashSet<>();
            records.forEach(record ->
                            {
                                if (record.type == type)
                                    ret.addAll(record.getTrackedFiles(parent.getParentFolder()));
                            });
            ret.add(file);
            return ret;
        }

        public void delete()
        {
            TransactionLog.delete(file);
        }

        public boolean exists()
        {
            return file.exists();
        }
    }

    /**
     * We split the transaction data from TransactionLog that implements the behavior
     * because we need to reconstruct any left-overs and clean them up, as well as work
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

        public Throwable readLogFile(Throwable accumulate)
        {
            try
            {
                file.readRecords();
            }
            catch (CorruptTransactionLogException ex)
            {
                logger.error("Possible disk corruption detected: failed to read corrupted transaction log {}", ex.file.file, ex);
                accumulate = merge(accumulate, ex);
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
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
                    file.deleteRecords(RecordType.OLD);
                else
                    file.deleteRecords(RecordType.NEW);

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
     * depending on the transaction result, as well as the transaction log file.
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
     * operations that were ongoing when the process exited. Also called by the standalone
     * sstableutil tool when the cleanup option is specified, @see StandaloneSSTableUtil.
     *
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
                    accumulate = data.readLogFile(accumulate);
                    if (accumulate == null)
                        accumulate = data.removeUnfinishedLeftovers(accumulate);
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
                    Throwable err = data.readLogFile(null);
                    if (err == null)
                        ret.addAll(data.getTemporaryFiles()
                                       .stream()
                                       .filter(file -> FileUtils.isContained(folder, file))
                                       .collect(Collectors.toSet()));
                }
            }
        }

        return ret;
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
