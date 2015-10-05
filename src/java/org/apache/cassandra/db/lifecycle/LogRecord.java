package org.apache.cassandra.db.lifecycle;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A decoded line in a transaction log file segment.
 *
 * @see LogFileSegment and LogFile.
 */
final class LogRecord
{
    public enum Type
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

        public boolean isFinal() { return this == Type.COMMIT || this == Type.ABORT; }

        public boolean isAny() { return !isFinal(); }
    }


    public final Type type;
    public final Optional<String> fileName;
    public final Optional<File> folder;
    public final long updateTime;
    public final int numFiles;
    public final String raw;
    public final long checksum;

    public String error;
    public LogRecord onDiskRecord;

    // (add|remove|commit|abort):[*,*,*][checksum]
    static Pattern REGEX = Pattern.compile("^(add|remove|commit|abort):\\[([^,]*),?([^,]*),?([^,]*)\\]\\[(\\d*)\\]$", Pattern.CASE_INSENSITIVE);

    public static LogRecord make(File file, String line)
    {
        try
        {
            Matcher matcher = REGEX.matcher(line);
            if (!matcher.matches())
                return new LogRecord(Type.UNKNOWN, null, "", 0, 0, 0, line)
                       .error(String.format("Failed to parse [%s]", line));

            Type type = Type.fromPrefix(matcher.group(1));
            return new LogRecord(type,
                                 file.getParentFile(),
                                 matcher.group(2),
                                 Long.valueOf(matcher.group(3)),
                                 Integer.valueOf(matcher.group(4)),
                                 Long.valueOf(matcher.group(5)), line);
        }
        catch (Throwable t)
        {
            return new LogRecord(Type.UNKNOWN, null, "", 0, 0, 0, line).error(t);
        }
    }

    public static LogRecord makeCommit(long updateTime)
    {
        return new LogRecord(Type.COMMIT, null, "", updateTime, 0);
    }

    public static LogRecord makeAbort(long updateTime)
    {
        return new LogRecord(Type.ABORT, null, "", updateTime, 0);
    }

    public static LogRecord make(Type type, SSTable table)
    {
        File folder = table.descriptor.directory;
        String fileName = FileUtils.getRelativePath(folder.getPath(), table.descriptor.baseFilename());
        return make(type, folder, getExistingFiles(folder, fileName), table.getAllFilePaths().size(), fileName);
    }

    public LogRecord withChangedFiles(List<File> files)
    {
        return make(type, folder.get(), files, 0, fileName.get());
    }

    public static LogRecord make(Type type, File folder, List<File> files, int minFiles, String fileName)
    {
        long lastModified = files.stream().map(File::lastModified).reduce(0L, Long::max);
        return new LogRecord(type, folder, fileName, lastModified, Math.max(minFiles, files.size()));
    }

    private LogRecord(Type type,
                      File folder,
                      String fileName,
                      long updateTime,
                      int numFiles)
    {
        this(type, folder, fileName, updateTime, numFiles, 0, null);
    }

    private LogRecord(Type type,
                      File folder,
                      String fileName,
                      long updateTime,
                      int numFiles,
                      long checksum,
                      String raw)
    {
        this.type = type;
        this.folder = type.hasFile() ? Optional.of(folder) : Optional.<File>empty();
        this.fileName = type.hasFile() ? Optional.of(fileName) : Optional.<String>empty();
        this.updateTime = type == Type.REMOVE ? updateTime : 0;
        this.numFiles = type.hasFile() ? numFiles : 0;
        if (raw == null)
        {
            assert checksum == 0;
            this.checksum = computeChecksum();
            this.raw = format();
        }
        else
        {
            this.checksum = checksum;
            this.raw = raw;
        }

        this.error = "";
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

    public boolean isValid()
    {
        return error.isEmpty() && type != Type.UNKNOWN;
    }

    private String format()
    {
        return String.format("%s:[%s,%d,%d][%d]",
                             type.toString(),
                             fileName.isPresent() ? fileName.get() : "",
                             updateTime,
                             numFiles,
                             checksum);
    }

    public List<File> getExistingFiles()
    {
        assert folder.isPresent() : "Expected a folder for a file type record";
        assert fileName.isPresent() : "Expected an sstable for a file type record";
        return getExistingFiles(folder.get(), fileName.get());
    }

    public static List<File> getExistingFiles(File parentFolder, String fileName)
    {
        return Arrays.asList(parentFolder.listFiles((dir, name) -> name.startsWith(fileName)));
    }

    public boolean isFinal()
    {
        return type.isFinal();
    }

    public boolean isAny()
    {
        return type.isAny();
    }

    @Override
    public int hashCode()
    {
        // see comment in equals
        return Objects.hash(type, folder, fileName, error);
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
        // however we must compare the error to make sure we have more than
        // one UNKNOWN record, if we fail to parse more than one
        return type == other.type &&
               folder.equals(other.folder) &&
               fileName.equals(other.fileName) &&
               error.equals(other.error);
    }

    @Override
    public String toString()
    {
        return raw;
    }

    /**
     * Compute the checksum. We could add the folder to the computation but
     * that would break compatibility with 3.0 rc1.
     */
    long computeChecksum()
    {
        CRC32 crc32 = new CRC32();
        crc32.update((fileName.isPresent() ? fileName.get() : "").getBytes(FileUtils.CHARSET));
        crc32.update(type.toString().getBytes(FileUtils.CHARSET));
        FBUtilities.updateChecksumInt(crc32, (int) updateTime);
        FBUtilities.updateChecksumInt(crc32, (int) (updateTime >>> 32));
        FBUtilities.updateChecksumInt(crc32, numFiles);
        return crc32.getValue() & (Long.MAX_VALUE);
    }
}
