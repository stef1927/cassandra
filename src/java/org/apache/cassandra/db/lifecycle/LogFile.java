package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.FileUtils;

/**
 * The transaction log file, which contains many records.
 */
final class LogFile
{
    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    static String EXT = ".log";
    static char SEP = '_';
    // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
    static Pattern FILE_REGEX = Pattern.compile(String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT));
    static Pattern LINE_REGEX = Pattern.compile("^(.*)\\[(\\d*)\\]$"); // *[checksum]

    public final File file;
    public final LogData parent;
    public final Set<LogRecord> records = new LinkedHashSet<>();
    public final Checksum checksum = new CRC32();

    LogFile(LogData parent)
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

    private LogRecord readRecord(String line)
    {
        Matcher matcher = LINE_REGEX.matcher(line);
        if (!matcher.matches())
        {
            return LogRecord.make(line).error(String.format("Failed to parse checksum in line '%s'", line));
        }

        LogRecord record = LogRecord.make(matcher.group(1));

        byte[] bytes = matcher.group(1).getBytes(FileUtils.CHARSET);
        checksum.update(bytes, 0, bytes.length);

        if (checksum.getValue() != Long.valueOf(matcher.group(2)))
            record.error(String.format("invalid line checksum %s for '%s'", matcher.group(2), line));

        return record;
    }

    public void checkRecords()
    {
        for (LogRecord record : records)
        {
            if (record.hasError())
                throw new LogTransaction.CorruptTransactionLogException(String.format("Record of transaction %s is corrupt [%s], aborting",
                                                                           parent.getId(),
                                                                           record.error), this);
        }
    }

    public void verifyRecords()
    {
        Optional<LogRecord> firstInvalid = records.stream()
                                                  .filter(this::isInvalid)
                                                  .findFirst();

        if (!firstInvalid.isPresent())
            return;

        LogRecord failedOn = firstInvalid.get();
        if (getLastRecord() != failedOn)
            throw new LogTransaction.CorruptTransactionLogException(String.format("Non-last record of transaction %s is corrupt [%s]; unexpected disk state, aborting", parent.getId(), failedOn.error), this);

        for (LogRecord r : records)
        {
            if (r == failedOn)
                break; // skip last record

            if (r.onDiskRecord == null)
                continue; // not a remove record

            if (r.onDiskRecord.numFiles < r.numFiles)
            { // if we found a corruption in the last record, then we continue only if the number of files matches exactly for all previous records.
                logger.error("Unexpected files detected for sstable [{}], record [{}]: number of files [{}] should have been [{}]",
                             r.relativeFilePath,
                             failedOn,
                             r.onDiskRecord.numFiles,
                             r.numFiles);

                throw new LogTransaction.CorruptTransactionLogException(String.format("Last record of transaction %s is corrupt [%s] and at least " +
                                                                           "one previous record does not match state on disk, unexpected disk state, aborting",
                                                                           parent.getId(),
                                                                           failedOn.error),
                                                             this);
            }

            // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
            // then we simply exited whilst serializing the last record and we carry on
            logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], but all previous records match state on disk; continuing",
                                      parent.getId(),
                                      failedOn.error));
        }
    }

    boolean isInvalid(LogRecord record)
    {
        return !verifyRecord(record);
    }

    public boolean verifyRecord(LogRecord record)
    {
        if (record.hasError())
            return false;

        if (record.type != Type.REMOVE)
            return true;

        List<File> files = record.getExistingFiles(parent.getFolder());

        // Paranoid sanity checks: we create another record by looking at the files as they are
        // on disk right now and make sure the information still matches
        record.onDiskRecord = LogRecord.make(record.type, files, 0, record.relativeFilePath);

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
        addRecord(LogRecord.makeCommit(System.currentTimeMillis()));
    }

    public void abort()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeAbort(System.currentTimeMillis()));
    }

    public boolean committed()
    {
        return records.contains(LogRecord.makeCommit(0));
    }

    public boolean aborted()
    {
        return records.contains(LogRecord.makeAbort(0));
    }

    public boolean completed()
    {
        return committed() || aborted();
    }

    public boolean add(Type type, SSTable table)
    {
        return addRecord(makeRecord(type, table));
    }

    private LogRecord makeRecord(Type type, SSTable table)
    {
        assert type == Type.ADD || type == Type.REMOVE;
        return LogRecord.make(type, parent.getFolder(), table);
    }

    private boolean addRecord(LogRecord record)
    {
        if (!records.add(record))
            return false;

        // we only checksum the records, not the checksums themselves
        byte[] bytes = record.getBytes();
        checksum.update(bytes, 0, bytes.length);

        FileUtils.append(file, String.format("%s[%d]", record, checksum.getValue()));

        parent.sync();
        return true;
    }

    public void remove(Type type, SSTable table)
    {
        LogRecord record = makeRecord(type, table);

        assert records.contains(record) : String.format("[%s] is not tracked by %s", record, file);

        records.remove(record);
        deleteRecord(record);
    }

    public boolean contains(Type type, SSTable table)
    {
        return records.contains(makeRecord(type, table));
    }

    public void deleteRecords(Type type)
    {
        assert file.exists() : String.format("Expected %s to exists", file);
        records.stream()
               .filter(type::matches)
               .forEach(this::deleteRecord);
        records.clear();
    }

    private void deleteRecord(LogRecord record)
    {
        List<File> files = record.getExistingFiles(parent.getFolder());
        if (files.isEmpty())
            return; // Files no longer exist, nothing to do

        // we sort the files in ascending update time order so that the last update time
        // stays the same even if we only partially delete files
        files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

        files.forEach(LogTransaction::delete);
    }

    public Map<LogRecord, Set<File>> getFilesOfType(NavigableSet<File> files, Type type)
    {
        Map<LogRecord, Set<File>> ret = new HashMap<>();

        for (LogRecord record : records)
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

    public LogRecord getLastRecord()
    {
        return Iterables.getLast(records, null);
    }

    private Set<File> getRecordFiles(NavigableSet<File> files, LogRecord record)
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
        LogTransaction.delete(file);
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

