package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.FileUtils;

/**
 * A log file record, each record is encoded in one line and has different
 * content depending on the record type.
 */
final class LogRecord
{
    private static final Logger logger = LoggerFactory.getLogger(LogRecord.class);

    public static enum Type
    {
        UNKNOWN, // a record that cannot be parsed
        ADD,    // new files to be retained on commit
        REMOVE, // old files to be retained on abort
        COMMIT, // commit flag
        ABORT;  // abort flag

        public static Type fromPrefix(String prefix)
        {
            return valueOf(prefix.toUpperCase());
        }

        public boolean hasFile()
        {
            return this == Type.ADD || this == Type.REMOVE;
        }

        public boolean matches(LogRecord record)
        {
            return this == record.type;
        }
    }


    public final Type type;
    public final String relativeFilePath;
    public final long updateTime;
    public final int numFiles;
    public final String record;

    public String error;
    public LogRecord onDiskRecord;

    static String REGEX_STR = "^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]$";
    static Pattern REGEX = Pattern.compile(REGEX_STR, Pattern.CASE_INSENSITIVE); // (add|remove|commit|abort):[*,*,*]

    public static LogRecord make(String record)
    {
        try
        {
            Matcher matcher = REGEX.matcher(record);
            if (!matcher.matches())
                throw new IllegalStateException("Corrupted record; does not match expected pattern " + REGEX_STR);

            Type type = Type.fromPrefix(matcher.group(1));
            return new LogRecord(type, matcher.group(2), Long.valueOf(matcher.group(3)), Integer.valueOf(matcher.group(4)), record);
        }
        catch (Throwable t)
        {
            logger.debug("Failed to create record {}", record, t);
            return new LogRecord(Type.UNKNOWN, "", 0, 0, record).error(t);
        }
    }

    public static LogRecord makeCommit(long updateTime)
    {
        return new LogRecord(Type.COMMIT, "", updateTime, 0, "");
    }

    public static LogRecord makeAbort(long updateTime)
    {
        return new LogRecord(Type.ABORT, "", updateTime, 0, "");
    }

    public static LogRecord make(Type type, String parentFolder, SSTable table)
    {
        String relativePath = FileUtils.getRelativePath(parentFolder, table.descriptor.baseFilename());
        // why do we take the max of files.size() and table.getAllFilePaths().size()?
        return make(type, getExistingFiles(parentFolder, relativePath), table.getAllFilePaths().size(), relativePath);
    }

    public static LogRecord make(Type type, List<File> files, int minFiles, String relativeFilePath)
    {
        long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
        return new LogRecord(type, relativeFilePath, lastModified, Math.max(minFiles, files.size()), "");
    }

    private LogRecord(Type type,
                      String relativeFilePath,
                      long updateTime,
                      int numFiles,
                      String record)
    {
        this.type = type;
        this.relativeFilePath = type.hasFile() ? relativeFilePath : ""; // only meaningful for file records
        this.updateTime = type == Type.REMOVE ? updateTime : 0; // only meaningful for old records
        this.numFiles = type.hasFile() ? numFiles : 0; // only meaningful for file records
        this.record = record.isEmpty() ? format() : record;
    }

    public LogRecord error(Throwable t)
    {
        return error(t.getMessage());
    }

    public LogRecord error(String error)
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
        if (!(obj instanceof LogRecord))
            return false;

        final LogRecord other = (LogRecord)obj;

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
