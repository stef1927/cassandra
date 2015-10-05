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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;

/**
 * Because a column family may have sstables on different disks and disks can
 * be removed, we split a LogFile into segments and store each segment in the
 * same folder as the sstables that it managed.
 *
 * This class is the abstraction for log file segments.
 *
 * Each segment contains the transaction records for the sstables located
 * in the same folder as well as the final commit or abort records.
 *
 * @see LogFile
 */
final class LogFileSegment
{
    private final int folderDescriptor;
    private final File file;

    static LogFileSegment create(File folder, String fileName)
    {
        return new LogFileSegment(CLibrary.tryOpenDirectory(folder.getPath()), new File(fileName));
    }

    static LogFileSegment open(File file)
    {
        return new LogFileSegment(CLibrary.tryOpenDirectory(file.getParentFile().getPath()), file);
    }

    LogFileSegment(int folderDescriptor, File file)
    {
        this.folderDescriptor = folderDescriptor;
        this.file = file;
    }

    File file()
    {
        return file;
    }

    void append(LogRecord record)
    {
        FileUtils.append(file, record.toString());
        sync();
    }

    void sync()
    {
        if (folderDescriptor >= 0)
            CLibrary.trySync(folderDescriptor);
    }

    void delete()
    {
        LogTransaction.delete(file);
    }

    List<LogRecord> readRecords()
    {
        return FileUtils.readLines(file).stream()
                        .map(l -> LogRecord.make(file, l))
                        .collect(Collectors.toList());
    }

    boolean exists()
    {
        return file.exists();
    }

    void close()
    {
        if (folderDescriptor >= 0)
            CLibrary.tryCloseFD(folderDescriptor);
    }

    @Override
    public String toString()
    {
        return file.toString();
    }
}
