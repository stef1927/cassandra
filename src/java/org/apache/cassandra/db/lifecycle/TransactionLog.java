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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SecureDirectoryStream;
import java.util.*;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Runnables;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.db.Directories.OnTxnErr;
import static org.apache.cassandra.db.Directories.FileType;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * IMPORTANT: When this object is involved in a transactional graph, and is not encapsulated in a LifecycleTransaction,
 * for correct behaviour its commit MUST occur before any others, since it may legitimately fail. This is consistent
 * with the Transactional API, which permits one failing action to occur at the beginning of the commit phase, but also
 * *requires* that the prepareToCommit() phase only take actions that can be rolled back.
 *
 * IMPORTANT: The transaction must complete (commit or abort) before any temporary files are deleted, even though the
 * txn log file itself will not be deleted until all tracked files are deleted. This is required by FileLister to ensure
 * a consistent disk state. LifecycleTransaction ensures this requirement, so this class should really never be used
 * outside of LT. @see FileLister.classifyFiles(TransactionData txn)
 *
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept; vice-versa if it fails.
 *
 * The transaction log file contains new and old sstables as follows:
 *
 * add:[sstable-2][CRC]
 * remove:[sstable-1,max_update_time,num files][CRC]
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
class TransactionLog extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(TransactionLog.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, TimeUnit.HOURS);

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

    public enum RecordType
    {
        UNKNOWN, // a record that cannot be parsed
        ADD,    // new files to be retained on commit
        REMOVE, // old files to be retained on abort
        COMMIT, // commit flag
        ABORT;  // abort flag

        public static RecordType fromPrefix(String prefix)
        {
            return valueOf(prefix.toUpperCase());
        }

        public boolean hasFile()
        {
            return this == RecordType.ADD || this == RecordType.REMOVE;
        }
    }

    /**
     * A log file record, each record is encoded in one line and has different
     * content depending on the record type.
     */
    final static class Record
    {
        public final RecordType type;
        public final String relativeFilePath;
        public final long updateTime;
        public final int numFiles;
        public final String record;

        public String error;
        public Record onDiskRecord;

        static String REGEX_STR = "^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]$";
        static Pattern REGEX = Pattern.compile(REGEX_STR, Pattern.CASE_INSENSITIVE); // (add|remove|commit|abort):[*,*,*]

        public static Record make(String record)
        {
            try
            {
                Matcher matcher = REGEX.matcher(record);
                if (!matcher.matches() || matcher.groupCount() != 4)
                {
                    logger.error("Failed to parse '{}', invalid record", record);
                    return new Record(RecordType.UNKNOWN, "", 0, 0, record).error(String.format("Failed to parse '%s'", record));
                }

                RecordType type = RecordType.fromPrefix(matcher.group(1));
                return new Record(type, matcher.group(2), Long.valueOf(matcher.group(3)), Integer.valueOf(matcher.group(4)), record);
            }
            catch (Throwable t)
            {
                logger.error("Failed to create record {}", record, t);

                int pos = record.indexOf(':');
                if (pos <= 0)
                    return new Record(RecordType.UNKNOWN, "", 0, 0, record).error(t);

                RecordType recordType;
                try
                {
                    recordType = RecordType.fromPrefix(record.substring(0, pos));
                }
                catch (Throwable tt)
                {
                    logger.error("Failed to parse record type {}", record.substring(0, pos), tt);
                    return new Record(RecordType.UNKNOWN, "", 0, 0, record).error(t);
                }

                return new Record(recordType, "", 0, 0, record).error(t);
            }
        }

        public static Record makeCommit(long updateTime)
        {
            return new Record(RecordType.COMMIT, "", updateTime, 0, "");
        }

        public static Record makeAbort(long updateTime)
        {
            return new Record(RecordType.ABORT, "", updateTime, 0, "");
        }

        public static Record make(RecordType type, String parentFolder, SSTable table)
        {
            String relativePath = FileUtils.getRelativePath(parentFolder, table.descriptor.baseFilename());
            List<File> files = getExistingFiles(parentFolder, relativePath);
            long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
            int numFiles = Math.max(files.size(), table.getAllFilePaths().size());
            return new Record(type, relativePath, lastModified, numFiles, "");
        }

        public static Record make(RecordType type, List<File> files, String relativeFilePath)
        {
            long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
            return new Record(type, relativeFilePath, lastModified, files.size(), "");
        }

        private Record(RecordType type,
                       String relativeFilePath,
                       long updateTime,
                       int numFiles,
                       String record)
        {
            this.type = type;
            this.relativeFilePath = type.hasFile() ? relativeFilePath : ""; // only meaningful for file records
            this.updateTime = type == RecordType.REMOVE ? updateTime : 0; // only meaningful for old records
            this.numFiles = type.hasFile() ? numFiles : 0; // only meaningful for file records
            this.record = record.isEmpty() ? format() : record;
        }

        public Record error(Throwable t)
        {
            return error(t.getMessage());
        }

        public Record error(String error)
        {
            this.error = error;
            return this;
        }

        public boolean hasError()
        {
            return this.error != null;
        }

        private String format()
        {
            return String.format("%s:[%s,%d,%d]", type.toString(), relativeFilePath, updateTime, numFiles);
        }

        public byte[] getBytes()
        {
            return record.getBytes(FileUtils.CHARSET);
        }

        public List<File> getExistingFiles(String parentFolder)
        {
            if (!type.hasFile())
                return Collections.emptyList();

            return getExistingFiles(parentFolder, relativeFilePath);
        }

        public static List<File> getExistingFiles(String parentFolder, String relativeFilePath)
        {
            return Arrays.asList(new File(parentFolder).listFiles((dir, name) -> name.startsWith(relativeFilePath)));
        }

        @Override
        public int hashCode()
        {
            // see comment in equals
            return Objects.hash(type, relativeFilePath);
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
            return type == other.type &&
                   relativeFilePath.equals(other.relativeFilePath);
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
        // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
        static String FILE_REGEX_STR = String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT);
        static Pattern FILE_REGEX = Pattern.compile(FILE_REGEX_STR);
        static String LINE_REGEX_STR = "^(.*)\\[(\\d*)\\]$"; // *[checksum]
        static Pattern LINE_REGEX = Pattern.compile(LINE_REGEX_STR);

        public final File file;
        public final TransactionData parent;
        public final LinkedHashSet<Record> records = new LinkedHashSet<>();
        public final Checksum checksum = new CRC32();

        TransactionFile(TransactionData parent)
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
            {
                return Record.make(line).error(String.format("Failed to parse checksum in line '%s'", line));
            }

            Record record = Record.make(matcher.group(1));

            byte[] bytes = matcher.group(1).getBytes(FileUtils.CHARSET);
            checksum.update(bytes, 0, bytes.length);

            if (checksum.getValue() != Long.valueOf(matcher.group(2)))
                record.error(String.format("invalid line checksum %s for '%s'", matcher.group(2), line));

            return record;
        }

        public void checkRecords()
        {
            for (Record record : records)
            {
                if (record.hasError())
                    throw new CorruptTransactionLogException(String.format("Record of transaction %s is corrupt [%s], aborting",
                                                                           parent.getId(),
                                                                           record.error), this);
            }
        }

        public void verifyRecords()
        {
            Iterator<Record> it = records.iterator();
            while(it.hasNext())
            {
                Record record = it.next();
                boolean isLast = !it.hasNext();

                if (!verifyRecord(record))
                {
                    if (isLast)
                    {
                        for (Record r : records)
                        {
                            if (r == record)
                                break; // skip last record

                            if (r.onDiskRecord == null)
                                continue; // not a remove record

                            if (r.onDiskRecord.numFiles < r.numFiles)
                            { // if we found a corruption in the last record, then we continue only if the number of files matches exactly for all previous records.
                                logger.error("Unexpected files detected for sstable [{}], record [{}]: number of files [{}] should have been [{}]",
                                             r.relativeFilePath,
                                             record,
                                             r.onDiskRecord.numFiles,
                                             r.numFiles);

                                throw new CorruptTransactionLogException(String.format("Last record of transaction %s is corrupt [%s] and at least " +
                                                                                       "one previous record does not match state on disk, unexpected disk state, aborting",
                                                                                       parent.getId(),
                                                                                       record.error),
                                                                         this);
                            }
                        }

                        // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
                        // then we simply exited whilst serializing the last record and we carry on
                        logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], but all previous records match state on disk; continuing",
                                                  parent.getId(),
                                                  record.error));

                    }
                    else
                    {
                        throw new CorruptTransactionLogException(String.format("Non-last record of transaction %s is corrupt [%s], unexpected disk state, aborting", parent.getId(), record.error), this);
                    }
                }
            }
        }

        public boolean verifyRecord(Record record)
        {
            if (record.hasError())
                return false;

            if (record.type != RecordType.REMOVE)
                return true;

            List<File> files = record.getExistingFiles(parent.getFolder());

            // Paranoid sanity checks: we create another record by looking at the files as they are
            // on disk right now and make sure the information still matches
            record.onDiskRecord = Record.make(record.type, files, record.relativeFilePath);

            if (record.updateTime != record.onDiskRecord.updateTime && record.onDiskRecord.numFiles > 0)
            {
                logger.error("Unexpected files detected for sstable [{}], record [{}]: last update time [{}] should have been [{}]",
                             record.relativeFilePath,
                             record,
                             new Date(record.onDiskRecord.updateTime),
                             new Date(record.updateTime));

                record.error("Failed to verify: unexpected sstable files");
                return false;
            }

            return true;
        }

        public void commit()
        {
            assert !completed() : "Already completed!";
            addRecord(Record.makeCommit(System.currentTimeMillis()));
        }

        public void abort()
        {
            assert !completed() : "Already completed!";
            addRecord(Record.makeAbort(System.currentTimeMillis()));
        }

        public boolean committed()
        {
            return records.contains(Record.makeCommit(0));
        }

        public boolean aborted()
        {
            return records.contains(Record.makeAbort(0));
        }

        public boolean completed()
        {
            return committed() || aborted();
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
            if (type == RecordType.ADD || type == RecordType.REMOVE)
            {
                return Record.make(type, parent.getFolder(), table);
            }
            else
            {
                throw new AssertionError("Invalid record type " + type);
            }
        }

        private void addRecord(Record record)
        {
            // we only checksum the records, not the checksums themselves
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
            records.stream()
                   .filter((r) -> r.type == type)
                   .forEach(this::deleteRecord);
            records.clear();
        }

        private void deleteRecord(Record record)
        {
            List<File> files = record.getExistingFiles(parent.getFolder());
            if (files.isEmpty())
                return; // Files no longer exist, nothing to do

            // we sort the files in ascending update time order so that the last update time
            // stays the same even if we only partially delete files
            files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

            files.forEach(TransactionLog::delete);
        }

        public Map<Record, Set<File>> getFilesOfType(NavigableSet<File> files, RecordType type)
        {
            Map<Record, Set<File>> ret = new HashMap<>();

            for (Record record : records)
            {
                if (record.type != type)
                    continue;

                if (record.hasError())
                {
                    logger.error("Ignoring record {} due to error {}", record, record.error);
                    continue;
                }

                ret.put(record, getRecordFiles(files, record));
            }

            return ret;
        }

        public Record getLastRecord()
        {
            return records.size() > 0 ? records.toArray(new Record[records.size()])[records.size() - 1] : null;
        }

        private Set<File> getRecordFiles(NavigableSet<File> files, Record record)
        {
            Set<File> ret = new HashSet<>();
            for (File file : files.tailSet(new File(parent.getFolder(), record.relativeFilePath)))
            {
                if (file.getName().startsWith(record.relativeFilePath))
                    ret.add(file);
                else
                    break;
            }
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

        @Override
        public String toString()
        {
            return FileUtils.getRelativePath(parent.getFolder(), FileUtils.getCanonicalPath(file));
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
            assert matcher.matches() && matcher.groupCount() == 3;

            // For now we don't need this but it is there in case we need to change
            // file format later on, the version is the sstable version as defined in BigFormat
            //String version = matcher.group(1);

            OperationType operationType = OperationType.fromFileName(matcher.group(2));
            UUID id = UUID.fromString(matcher.group(3));

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
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        public Throwable verify(Throwable accumulate)
        {
            try
            {
                file.verifyRecords();
            }
            catch (Throwable t)
            {
                accumulate = merge(accumulate, t);
            }

            return accumulate;
        }

        public Throwable check(Throwable accumulate)
        {
            try
            {
                file.checkRecords();
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

        boolean completed()
        {
            return  file.completed();
        }

        Throwable removeUnfinishedLeftovers(Throwable accumulate)
        {
            try
            {
                if (file.committed())
                    file.deleteRecords(RecordType.REMOVE);
                else
                    file.deleteRecords(RecordType.ADD);

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

        String getFileName()
        {
            String fileName = StringUtils.join(BigFormat.latestVersion,
                                               TransactionFile.SEP,
                                               "txn",
                                               TransactionFile.SEP,
                                               opType.fileName,
                                               TransactionFile.SEP,
                                               id.toString(),
                                               TransactionFile.EXT);
            return StringUtils.join(folder, File.separator, fileName);
        }

        String getFolder()
        {
            return folder.getPath();
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

        @Override
        public String toString()
        {
            return String.format("[%s]", file.toString());
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
                                        folder,
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
        if (!data.file.add(RecordType.ADD, table))
            throw new IllegalStateException(table + " is already tracked as new");
    }

    /**
     * Stop tracking a reader as new.
     */
    void untrackNew(SSTable table)
    {
        data.file.remove(RecordType.ADD, table);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced.
     */
    SSTableTidier obsoleted(SSTableReader reader)
    {
        if (data.file.contains(RecordType.ADD, reader))
        {
            if (data.file.contains(RecordType.REMOVE, reader))
                throw new IllegalArgumentException();

            return new SSTableTidier(reader, true, this);
        }

        if (!data.file.add(RecordType.REMOVE, reader))
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
        return data.getFolder();
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

        TransactionTidier(TransactionData data)
        {
            this.data = data;
        }

        public void tidy() throws Exception
        {
            run();
        }

        public String name()
        {
            return data.toString();
        }

        public void run()
        {
            if (logger.isDebugEnabled())
                logger.debug("Removing files for transaction {}", name());

            assert data.completed() : "Expected a completed transaction: " + data;

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

        Obsoletion(SSTableReader reader, SSTableTidier tidier)
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


    static void rescheduleFailedDeletions()
    {
        Runnable task;
        while ( null != (task = failedDeletions.poll()))
            ScheduledExecutors.nonPeriodicTasks.submit(task);

        // On Windows, snapshots cannot be deleted so long as a segment of the root element is memory-mapped in NTFS.
        SnapshotDeletingTask.rescheduleFailedTasks();
    }

    static void waitForDeletions()
    {
        FBUtilities.waitOnFuture(ScheduledExecutors.nonPeriodicTasks.schedule(Runnables.doNothing(), 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    Throwable complete(Throwable accumulate)
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
        data.file.abort();
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

        for (File dir : new Directories(metadata).getCFDirectories())
        {
            File[] logs = dir.listFiles((dir1, name) -> TransactionData.isLogFile(name));

            for (File log : logs)
            {
                try (TransactionData data = TransactionData.make(log))
                {
                    accumulate = data.verify(data.readLogFile(accumulate));
                    if (accumulate == null)
                        accumulate = data.removeUnfinishedLeftovers(null);
                    else
                        logger.error("Unexpected disk state: failed to read transaction log {}", log, accumulate);
                }
            }
        }

        if (accumulate != null)
            logger.error("Failed to remove unfinished transaction leftovers", accumulate);
    }

    /**
     * A class for listing files in a folder.
     */
    static final class FileLister
    {
        // The folder to scan
        private final Path folder;

        // The filter determines which files the client wants returned
        private final BiFunction<File, FileType, Boolean> filter; //file, file type

        // The behavior when we fail to list files
        private final OnTxnErr onTxnErr;

        // The unfiltered result
        NavigableMap<File, FileType> files = new TreeMap<>();

        // A flag to force non-atomic listing on platforms that support atomic listing, used for testing only
        private final boolean forceNotAtomic;

        FileLister(Path folder, BiFunction<File, FileType, Boolean> filter, OnTxnErr onTxnErr)
        {
            this(folder, filter, onTxnErr, false);
        }

        @VisibleForTesting
        FileLister(Path folder, BiFunction<File, FileType, Boolean> filter, OnTxnErr onTxnErr, boolean forceNotAtomic)
        {
            this.folder = folder;
            this.filter = filter;
            this.onTxnErr = onTxnErr;
            this.forceNotAtomic = forceNotAtomic;
        }

        public List<File> list()
        {
            try
            {
                return innerList();
            }
            catch (Throwable t)
            {
                throw new RuntimeException(String.format("Failed to list files in %s", folder), t);
            }
        }

        List<File> innerList() throws Throwable
        {
            if (!files.isEmpty())
                files.clear();

            Set<File> txnLogs = new HashSet<>(); // the txn log files
            boolean isAtomicStream; // whether this platform supports atomic file listing in a folder

            try (DirectoryStream<Path> in = Files.newDirectoryStream(folder))
            {
                isAtomicStream = !forceNotAtomic && (in instanceof SecureDirectoryStream);

                for (Path path : in)
                {
                    File file = path.toFile();
                    if (file.isDirectory())
                        continue;

                    if (TransactionData.isLogFile(file.getName()))
                    {
                        // For atomic streams we can add txn log files during the first pass
                        if (isAtomicStream)
                            txnLogs.add(file);

                        continue;
                    }

                   files.put(file, FileType.FINAL);
                }
            }

            // If the file stream is not atomic we cannot be sure we have listed a consistent
            // disk state, so we must be careful to list txn log files AFTER every other file
            // since these files are deleted last, after all other files are removed
            if (!isAtomicStream)
            {
                noSpamLogger.info("This platform does not support atomic directory streams (SecureDirectoryStream)");
                try (DirectoryStream<Path> in = Files.newDirectoryStream(folder, '*' + TransactionFile.EXT))
                {
                    for (Path path : in)
                    {
                        File file = path.toFile();
                        if (TransactionData.isLogFile(file.getName()))
                            txnLogs.add(file);
                        else
                            logger.error("Ignoring unexpected file {}", file);
                    }
                }
            }

            // Now we read the txn log files and classify files according to txn state
            for (File log : txnLogs)
                classifyFiles(log);

            // Finally we apply the user filter before returning our result
            return files.entrySet().stream()
                      .filter((e) -> filter.apply(e.getKey(), e.getValue()))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toList());
        }

        /**
         * We read txn log files, if we fail we throw only if the user has specified
         * OnTxnErr.THROW, else we log an error and apply the txn log anyway
         */
        void classifyFiles(File txnFile) throws Throwable
        {
            try (TransactionData txn = TransactionData.make(txnFile))
            {
                readTxnLog(txn);
                classifyFiles(txn);
                files.put(txnFile, FileType.TXN_LOG);
            }
        }

        void readTxnLog(TransactionData txn) throws Throwable
        {
            Throwable err = txn.check(txn.readLogFile(null));
            if (err != null)
            {
                if (onTxnErr == OnTxnErr.THROW)
                    throw err;

                logger.error("Failed to read temporary files of txn {}: {}", txn, err.getMessage());
            }
        }

        void classifyFiles(TransactionData txn) throws Throwable
        {
            TransactionFile txnFile = txn.file;

            Map<Record, Set<File>> oldFiles = txnFile.getFilesOfType(files.navigableKeySet(), RecordType.REMOVE);
            Map<Record, Set<File>> newFiles = txnFile.getFilesOfType(files.navigableKeySet(), RecordType.ADD);

            if (txnFile.completed())
            { // last record present, filter regardless of disk status
                filter(txnFile, oldFiles.values(), newFiles.values());
                return;
            }

            if (allFilesPresent(txnFile, oldFiles, newFiles))
            {  // all files present, transaction is in progress, this will filter as aborted
                filter(txnFile, oldFiles.values(), newFiles.values());
                return;
            }

            // some files are missing, we expect the txn file to either also be missing or completed, so check
            // disk state again to resolve any previous races on non-atomic directory listing platforms

            // if txn file also gone, then do nothing (all temporary should be gone, we could remove them if any)
            if (!txn.file.exists())
                return;

            // otherwise read the file again to see if it is completed now
            readTxnLog(txn);

            if (txn.completed())
            { // if after re-reading the txn is completed then filter accordingly
                filter(txnFile, oldFiles.values(), newFiles.values());
                return;
            }

            // some files are missing and yet the txn is still there and not completed
            // something must be wrong (see comment at the top of this file requiring txn to be
            // completed before obsoleting or aborting sstables)
            throw new RuntimeException(String.format("Failed to list directory files in %s, inconsistent disk state for transaction %s",
                                                     folder,
                                                     txn));
        }

        /** See if all files are present or if only the last record files are missing and it's a NEW record */
        private boolean allFilesPresent(TransactionFile txnFile, Map<Record, Set<File>> oldFiles, Map<Record, Set<File>> newFiles)
        {
            boolean allPresent = true;

            for (Map.Entry<Record, Set<File>> entry : oldFiles.entrySet())
            {
                if (entry.getKey().numFiles > entry.getValue().size())
                {
                    allPresent = false;
                    break;
                }
            }

            if (allPresent)
            {
                Record lastRecord = txnFile.getLastRecord();
                for (Map.Entry<Record, Set<File>> entry : newFiles.entrySet())
                {
                    if (entry.getKey() == lastRecord)
                        continue; //skip last record for NEW files

                    if (entry.getKey().numFiles > entry.getValue().size())
                    { // this would happen also in the case of some files not yet created but we skip the last record
                      // so it should never occur that some files are not yet created
                        allPresent = false;
                        break;
                    }
                }
            }

            return allPresent;
        }

        private void filter(TransactionFile txnFile, Collection<Set<File>> oldFiles, Collection<Set<File>> newFiles)
        {
            if (txnFile.committed())
            {
                for (Set<File> files : oldFiles)
                    files.forEach((f) -> this.files.put(f, FileType.TEMPORARY));
            }
            else
            { // aborted or in progress
                for (Set<File> files : newFiles)
                    files.forEach((f) -> this.files.put(f, FileType.TEMPORARY));
            }
        }

    }

    @VisibleForTesting
    static Set<File> getTemporaryFiles(CFMetaData metadata, File folder)
    {
        return getTemporaryFiles(metadata, folder, false);
    }

    @VisibleForTesting
    static Set<File> getTemporaryFiles(CFMetaData metadata, File folder, boolean forceNonAtomicListing)
    {
        return listFiles(metadata, folder, forceNonAtomicListing, FileType.TEMPORARY);
    }

    @VisibleForTesting
    static Set<File> listFiles(CFMetaData metadata, File folder, boolean forceNonAtomicListing, final FileType ... types)
    {
        Set<File> ret = new HashSet<>();

        List<File> directories = new Directories(metadata).getCFDirectories();
        directories.add(folder);
        for (File dir : directories)
            ret.addAll(new FileLister(dir.toPath(),
                                      (file, type) ->
                                      {
                                          for (FileType t : types)
                                              if (t == type)
                                                  return true;
                                          return false;
                                      },
                                      OnTxnErr.IGNORE,
                                      forceNonAtomicListing).list());

        return ret;
    }
}
