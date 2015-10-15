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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;

/**
 * Because a column family may have sstables on different disks and disks can
 * be removed, we duplicate log files into many replicas so as to have a file
 * in each folder where sstables exist.
 *
 * Each replica contains the exact same content but we do allow for final
 * partial records in case we crashed after writing to one replica but
 * before compliting the write to another replica.
 *
 * @see LogFile
 */
final class LogReplica
{
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private final File file;
    private int folderDescriptor;
    private BufferedWriter writer;

    static LogReplica create(File folder, String fileName)
    {
        return new LogReplica(new File(fileName), CLibrary.tryOpenDirectory(folder.getPath()));
    }

    static LogReplica open(File file)
    {
        return new LogReplica(file, CLibrary.tryOpenDirectory(file.getParentFile().getPath()));
    }

    LogReplica(File file, int folderDescriptor)
    {
        this.file = file;
        this.folderDescriptor = folderDescriptor;
    }

    File file()
    {
        return file;
    }

    void append(LogRecord record)
    {
        try
        {
            if (writer == null)
            {
                writer = new BufferedWriter(new FileWriter(file, true));
                sync();
            }

            writer.append(record.toString());
            writer.append(LINE_SEPARATOR);
            writer.flush();
        }
        catch (IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    void sync()
    {
        if (folderDescriptor >= 0)
            CLibrary.trySync(folderDescriptor);
    }

    void delete()
    {
        FileUtils.closeQuietly(writer);
        writer = null;

        LogTransaction.delete(file);
        sync();
    }

    boolean exists()
    {
        return file.exists();
    }

    void close()
    {
        FileUtils.closeQuietly(writer);
        writer = null;

        if (folderDescriptor >= 0)
        {
            CLibrary.tryCloseFD(folderDescriptor);
            folderDescriptor = -1;
        }
    }

    @Override
    public String toString()
    {
        return String.format("[%s] ", file);
    }
}
