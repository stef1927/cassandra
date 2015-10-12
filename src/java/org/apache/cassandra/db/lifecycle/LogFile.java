package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.utils.Throwables.merge;

/**
 * A transaction log file. We store transaction records into a log file, which is
 * split into multiple segments on different disks, @see LogFileSegment.
 *
 * This class is a consolidated view of all records stored on disk. It supports
 * the transactional logic of LogTransaction and the removing of unfinished leftovers
 * when a transaction is completed, or aborted, or when we clean up on start-up.
 *
 * @see LogTransaction
 */
final class LogFile
{
    private static final Logger logger = LoggerFactory.getLogger(LogFile.class);

    static String EXT = ".log";
    static char SEP = '_';
    // cc_txn_opname_id.log (where cc is one of the sstable versions defined in BigVersion)
    static Pattern FILE_REGEX = Pattern.compile(String.format("^(.{2})_txn_(.*)_(.*)%s$", EXT));


    private final Map<File, LogFileSegment> segments = new LinkedHashMap<>();
    private final Set<LogRecord> records = new LinkedHashSet<>();
    private final OperationType type;
    private final UUID id;

    static LogFile make(File logFile)
    {
        return make(logFile.getName(), Collections.singletonList(logFile));
    }

    static LogFile make(String fileName, List<File> logFiles)
    {
        Matcher matcher = LogFile.FILE_REGEX.matcher(fileName);
        boolean matched = matcher.matches();
        assert matched && matcher.groupCount() == 3;

        // For now we don't need this but it is there in case we need to change
        // file format later on, the version is the sstable version as defined in BigFormat
        //String version = matcher.group(1);

        OperationType operationType = OperationType.fromFileName(matcher.group(2));
        UUID id = UUID.fromString(matcher.group(3));

        return new LogFile(operationType, id, logFiles);
    }

    private Collection<LogFileSegment> segments()
    {
        return segments.values();

    }

    Throwable sync(Throwable accumulate)
    {
        return Throwables.perform(accumulate, segments().stream().map(s -> () -> s.sync()));
    }

    OperationType type()
    {
        return type;
    }

    UUID id()
    {
        return id;
    }

    Throwable removeUnfinishedLeftovers(Throwable accumulate)
    {
        try
        {
            deleteRecords(committed() ? Type.REMOVE : Type.ADD);

            // we sync the parent file descriptors between contents and log deletion
            // to ensure there is a happens before edge between them
            Throwables.maybeFail(sync(accumulate));

            accumulate = Throwables.perform(accumulate, segments().stream().map(s -> s::delete));
        }
        catch (Throwable t)
        {
            accumulate = merge(accumulate, t);
        }

        return accumulate;
    }

    static boolean isLogFile(File file)
    {
        return LogFile.FILE_REGEX.matcher(file.getName()).matches();
    }

    LogFile(OperationType type, UUID id, List<File> segments)
    {
        this(type, id);
        addSegments(segments);
    }

    LogFile(OperationType type, UUID id)
    {
        this.type = type;
        this.id = id;
    }

    private void addSegments(List<File> segments)
    {
        segments.forEach(this::addSegment);
    }

    private void addSegment(File file)
    {
        File folder = file.getParentFile();
        assert !segments.containsKey(folder);
        segments.put(folder, LogFileSegment.open(file));

        if (logger.isTraceEnabled())
            logger.trace("Added log file segment {} ", file);
    }

    private void maybeCreateSegment(File folder)
    {
        LogFileSegment ret = segments.get(folder);
        if (ret != null)
            return;

        ret = LogFileSegment.create(folder, getFileName(folder));
        segments.put(folder, ret);

        if (logger.isTraceEnabled())
            logger.trace("Created new file segment {}", ret);
    }

    boolean verify()
    {
        if (!readRecords())
        {
            logger.error("Failed to read records for txn {} with {}", id, segments());
            return false;
        }

        Optional<LogRecord> firstInvalid = records.stream()
                                                  .filter(LogFile::isInvalid)
                                                  .findFirst();

        if (!firstInvalid.isPresent())
            return true;

        LogRecord failedOn = firstInvalid.get();
        if (getLastRecord() != failedOn)
        {
            logError(failedOn);
            return false;
        }

        if (records.stream()
                   .filter((r) -> r != failedOn)
                   .filter(LogFile::isInvalidWithCorruptedLastRecord)
                   .map(LogFile::logError)
                   .findFirst().isPresent())
        {
            logError(failedOn);
            return false;
        }

        // if only the last record is corrupt and all other records have matching files on disk, @see verifyRecord,
        // then we simply exited whilst serializing the last record and we carry on
        logger.warn(String.format("Last record of transaction %s is corrupt or incomplete [%s], " +
                                  "but all previous records match state on disk; continuing",
                                  id,
                                  failedOn.error));
        return true;
    }

    private boolean readRecords()
    {
        records.clear();

        LogRecord finalRecord = null;
        boolean missingFinalRecord = false;
        for (LogFileSegment segment : segments())
        {
            if (logger.isTraceEnabled())
                logger.trace("Reading txn segment {}", segment);

            // add ordinary records and keep final records (commit flag)
            // these should match but it's OK to only warn if some segments
            // are missing the final record
            List<LogRecord> finalRecords = segment.readRecords()
                                                  .stream()
                                                  .map(r -> {
                                                      if (r.isAny()) records.add(r);
                                                      return r;
                                                  })
                                                  .filter(LogRecord::isFinal)
                                                  .collect(Collectors.toList());
            if (finalRecords.size() > 1)
            {
                logger.error("{} has more than one final record", segment);
                return false; // this should really never happen
            }
            else if (finalRecords.isEmpty())
            {
                missingFinalRecord = true;
            }
            else
            {
                if (finalRecord == null)
                {
                    finalRecord = finalRecords.get(0);
                }
                else if (!finalRecord.equals(finalRecords.get(0)))
                {
                    logger.error("Final records do not match: {} / {}", finalRecord, finalRecords.get(0));
                    return false;
                }
            }
        }

        if (missingFinalRecord && finalRecord != null)
            logger.warn("Some txn segments for {} were missing the final record, assuming {} - segments: {}",
                        id,
                        finalRecord,
                        segments());

        if (finalRecord != null)
            records.add(finalRecord);

        return true;
    }

    static LogRecord logError(LogRecord record)
    {
        logger.error("{}", record.error);
        return record;
    }

    static boolean isInvalid(LogRecord record)
    {
        if (!record.isValid())
            return true;

        if (record.checksum != record.computeChecksum())
        {
            record.error(String.format("Invalid checksum for sstable [%s], record [%s]: [%d] should have been [%d]",
                                       record.fileName,
                                       record,
                                       record.checksum,
                                       record.computeChecksum()));
            return true;
        }

        if (record.type != Type.REMOVE)
            return false;

        List<File> files = record.getExistingFiles();

        // Paranoid sanity checks: we create another record by looking at the files as they are
        // on disk right now and make sure the information still matches. We don't want to delete
        // files by mistake if the user has copied them from backup and forgot to remove a txn log
        // file that obsoleted the very same files. So we check the latest update time and make sure
        // it matches. Because we delete files from oldest to newest, the latest update time should
        // always match.
        record.onDiskRecord = record.withChangedFiles(files);

        if (record.updateTime != record.onDiskRecord.updateTime && record.onDiskRecord.numFiles > 0)
        {
            record.error(String.format("Unexpected files detected for sstable [%s], " +
                                       "record [%s]: last update time [%tT] should have been [%tT]",
                                       record.fileName,
                                       record,
                                       record.onDiskRecord.updateTime,
                                       record.updateTime));
            return true;
        }

        return false;
    }

    static boolean isInvalidWithCorruptedLastRecord(LogRecord record)
    {
        if (record.type == Type.REMOVE && record.onDiskRecord.numFiles < record.numFiles)
        { // if we found a corruption in the last record, then we continue only
          // if the number of files matches exactly for all previous records.
            record.error(String.format("Incomplete fileset detected for sstable [%s], record [%s]: " +
                                       "number of files [%d] should have been [%d]. Treating as unrecoverable " +
                                       "due to corruption of the final record.",
                         record.fileName,
                         record.raw,
                         record.onDiskRecord.numFiles,
                         record.numFiles));
            return true;
        }
        return false;
    }

    void commit()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeCommit(System.currentTimeMillis()));
    }

    void abort()
    {
        assert !completed() : "Already completed!";
        addRecord(LogRecord.makeAbort(System.currentTimeMillis()));
    }

    private boolean isLastRecordValidWithType(Type type)
    {
        LogRecord lastRecord = getLastRecord();
        return lastRecord != null &&
               lastRecord.type == type &&
               !isInvalid(lastRecord);
    }

    boolean committed()
    {
        return isLastRecordValidWithType(Type.COMMIT);
    }

    boolean aborted()
    {
        return isLastRecordValidWithType(Type.ABORT);
    }

    boolean completed()
    {
        return committed() || aborted();
    }

    void add(Type type, SSTable table)
    {
        if (!addRecord(makeRecord(type, table)))
            throw new IllegalStateException();
    }

    private LogRecord makeRecord(Type type, SSTable table)
    {
        assert type == Type.ADD || type == Type.REMOVE;

        File folder = table.descriptor.directory;
        maybeCreateSegment(folder);
        return LogRecord.make(type, table);
    }

    private boolean addRecord(LogRecord record)
    {
        if (!records.add(record))
            return false;

        if (!record.folder.isPresent())
        {   // here we write the record to multiple files, this is typically for the final
            // commit or abort flag, so we only throw if we fail to add the record to ALL files
            Throwable err = Throwables.perform(null, segments().stream().map(s -> () -> s.append(record)));
            if (err != null)
            {
                if (err.getSuppressed().length == segments().size() -1)
                    Throwables.maybeFail(err); // all failed

                logger.error("Failed to add record {} to some segments [{}]", record, segments());
            }
        }
        else
        {   // here we only write the record to one file, so this will throw if we fail to do so
            segments.get(record.folder.get()).append(record);
        }

        return true;
    }

    void remove(Type type, SSTable table)
    {
        LogRecord record = makeRecord(type, table);
        assert records.contains(record) : String.format("[%s] is not tracked by %s", record, id);

        records.remove(record);
        deleteRecord(record);
    }

    boolean contains(Type type, SSTable table)
    {
        return records.contains(makeRecord(type, table));
    }

    void deleteRecords(Type type)
    {
        assert !segments.isEmpty() : "Expected at least one log file to exist";
        records.stream()
               .filter(type::matches)
               .forEach(LogFile::deleteRecord);
        records.clear();
    }

    private static void deleteRecord(LogRecord record)
    {
        List<File> files = record.getExistingFiles();

        // we sort the files in ascending update time order so that the last update time
        // stays the same even if we only partially delete files, see comment in isInvalid()
        files.sort((f1, f2) -> Long.compare(f1.lastModified(), f2.lastModified()));

        files.forEach(LogTransaction::delete);
    }

    Map<LogRecord, Set<File>> getFilesOfType(NavigableSet<File> files, Type type)
    {
        Map<LogRecord, Set<File>> ret = new HashMap<>();

        records.stream()
               .filter(type::matches)
               .filter(LogRecord::isValid)
               .forEach((r) -> ret.put(r, getRecordFiles(files, r)));

        return ret;
    }

    LogRecord getLastRecord()
    {
        return Iterables.getLast(records, null);
    }

    private static Set<File> getRecordFiles(NavigableSet<File> files, LogRecord record)
    {
        assert record.folder.isPresent() : "Expected a folder in order to get record files";
        assert record.fileName.isPresent() : "Expected an sstable in order to get record files";

        File folder = record.folder.get();
        String fileName = record.fileName.get();

        Set<File> ret = new HashSet<>();
        for (File file : files.tailSet(new File(folder, fileName)))
        {
            if (!file.getName().startsWith(fileName))
                break;

            ret.add(file);
        }
        return ret;
    }

    boolean exists()
    {
        return !segments.isEmpty() && segments().stream().map(LogFileSegment::exists).reduce(Boolean::logicalAnd).get();
    }

    void close()
    {
        segments().forEach(LogFileSegment::close);
    }

    @Override
    public String toString()
    {
        return id.toString();
    }

    private String getFileName(File folder)
    {
        String fileName = StringUtils.join(BigFormat.latestVersion,
                                           LogFile.SEP,
                                           "txn",
                                           LogFile.SEP,
                                           type.fileName,
                                           LogFile.SEP,
                                           id.toString(),
                                           LogFile.EXT);
        return StringUtils.join(folder, File.separator, fileName);
    }

    @VisibleForTesting
    List<File> getFiles()
    {
        return segments().stream().map(LogFileSegment::file).collect(Collectors.toList());
    }

    @VisibleForTesting
    List<String> getFilePaths()
    {
        return segments().stream().map(LogFileSegment::file).map(File::getPath).collect(Collectors.toList());
    }
}
