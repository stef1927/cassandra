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
    private final int folderDescriptor;
    private final File file;

    static LogReplica create(File folder, String fileName)
    {
        return new LogReplica(CLibrary.tryOpenDirectory(folder.getPath()), new File(fileName));
    }

    static LogReplica open(File file)
    {
        return new LogReplica(CLibrary.tryOpenDirectory(file.getParentFile().getPath()), file);
    }

    LogReplica(int folderDescriptor, File file)
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
        return String.format("[%s] ", file);
    }
}
