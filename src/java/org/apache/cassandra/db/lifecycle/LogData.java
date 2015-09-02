package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.UUID;
import java.util.regex.Matcher;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.utils.CLibrary;

import static org.apache.cassandra.db.lifecycle.LogRecord.*;
import static org.apache.cassandra.utils.Throwables.merge;

/**
 * We split the transaction data from TransactionLog that implements the behavior
 * because we need to reconstruct any left-overs and clean them up, as well as work
 * out which files are temporary. So for these cases we don't want the full
 * transactional behavior, plus it's handy for the TransactionTidier.
 */
final class LogData implements AutoCloseable
{
    final OperationType opType;
    final UUID id;
    final File folder;
    final LogFile file;
    private int folderDescriptor;

    static LogData make(File logFile)
    {
        Matcher matcher = LogFile.FILE_REGEX.matcher(logFile.getName());
        assert matcher.matches() && matcher.groupCount() == 3;

        // For now we don't need this but it is there in case we need to change
        // file format later on, the version is the sstable version as defined in BigFormat
        //String version = matcher.group(1);

        OperationType operationType = OperationType.fromFileName(matcher.group(2));
        UUID id = UUID.fromString(matcher.group(3));

        return new LogData(operationType, logFile.getParentFile(), id);
    }

    LogData(OperationType opType, File folder, UUID id)
    {
        this.opType = opType;
        this.id = id;
        this.folder = folder;
        this.file = new LogFile(this);
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
                file.deleteRecords(Type.REMOVE);
            else
                file.deleteRecords(Type.ADD);

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
                                           LogFile.SEP,
                                           "txn",
                                           LogFile.SEP,
                                           opType.fileName,
                                           LogFile.SEP,
                                           id.toString(),
                                           LogFile.EXT);
        return StringUtils.join(folder, File.separator, fileName);
    }

    String getFolder()
    {
        return folder.getPath();
    }

    static boolean isLogFile(File file)
    {
        return LogFile.FILE_REGEX.matcher(file.getName()).matches();
    }

    @VisibleForTesting
    LogFile getLogFile()
    {
        return file;
    }

    @Override
    public String toString()
    {
        return String.format("[%s]", file.toString());
    }
}
