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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
 * A class that tracks sstable files involved in a transaction across sstables:
 * if the transaction succeeds the old files should be deleted and the new ones kept; vice-versa if it fails.
 *
 * Two log files, NEW and OLD, contain new and old sstable files respectively. The log files also track each
 * other by referencing each others path in the contents.
 *
 * If the transaction finishes successfully:
 * - the OLD transaction file is deleted along with its contents, this includes the NEW transaction file.
 *   Before deleting we must let the SSTableTidier instances run first for any old readers that are being obsoleted
 *   (mark as compacted) by the transaction, see LifecycleTransaction
 *
 * If the transaction is aborted:
 * - the NEW transaction file and its contents are deleted, this includes the OLD transaction file
 *
 * On start-up:
 * - If we find a NEW transaction file, it means the transaction did not complete and we delete the NEW file and its contents
 * - If we find an OLD transaction file but not a NEW file, it means the transaction must have completed and so we delete
 *   all the contents of the OLD file, if they still exist, and the OLD file itself.
 *
 * See CASSANDRA-7066 for full details.
 */
public class TransactionLogs extends Transactional.AbstractTransactional implements Transactional
{
    // stef: i'm not too convinced by exposing the same executor again, seems a little confusing to me without any value add
    // stashing it for local
    private static final ScheduledThreadPoolExecutor executor = ScheduledExecutors.nonPeriodicTasks;
    private static final Logger logger = LoggerFactory.getLogger(TransactionLogs.class);

    /**
     * A single transaction log file, either NEW or OLD.
     */
    final static class TransactionFile
    {
        static String EXT = ".log";
        static char SEP = '_';
        static String REGEX_STR = String.format("^(.*)_(.*)_(%s|%s)%s$", Type.NEW.txt, Type.OLD.txt, EXT);
        static Pattern REGEX = Pattern.compile(REGEX_STR); //(opname)_(id)_(new|old).data

        public enum Type
        {
            NEW (0, "new"),
            OLD (1, "old");

            public final int idx;
            public final String txt;

            Type(int idx, String txt)
            {
                this.idx = idx;
                this.txt = txt;
            }
        };

        public final Type type;
        public final File file;
        public final TransactionData parent;
        public final Set<String> lines = new HashSet<>();

        public TransactionFile(Type type, TransactionData parent)
        {
            this.type = type;
            this.file = new File(parent.getFileName(type));
            this.parent = parent;

            if (exists())
                lines.addAll(FileUtils.readLines(file));
        }

        public void add(SSTable table)
        {
            add(table.descriptor.baseFilename());
        }

        private void add(String path)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), path);
            if (!lines.contains(relativePath))
            {
                lines.add(relativePath);
                FileUtils.append(file, relativePath);
            }
        }

        public boolean remove(SSTable table, boolean delete)
        {
            String relativePath = FileUtils.getRelativePath(parent.getParentFolder(), table.descriptor.baseFilename());
            if (!lines.contains(relativePath))
                return false;

            lines.remove(relativePath);
            if (delete)
                delete(relativePath);

            String[] linesArray = new String[lines.size()];
            FileUtils.replace(file, lines.toArray(linesArray));
            return true;
        }

        private void deleteContents()
        {
            lines.forEach(line -> delete(line));
        }

        private void delete(String relativePath)
        {
            getTrackedFiles(relativePath).forEach(file -> delete(file));
        }

        static void delete(File file)
        {
            try
            {
                if (logger.isDebugEnabled())
                    logger.debug("Deleting {}", file);

                Files.delete(file.toPath());
            }
            catch (NoSuchFileException e)
            {
                logger.warn("Unable to delete {} as it does not exist", file);
            }
            catch (IOException e)
            {
                logger.error("Unable to delete {}", file, e);
                throw new RuntimeException(e);
            }
        }

        public Set<File> getTrackedFiles()
        {
            Set<File> ret = new HashSet<>();
            FileUtils.readLines(file).forEach(line -> ret.addAll(getTrackedFiles(line)));
            ret.add(file);
            return ret;
        }

        private List<File> getTrackedFiles(String relativePath)
        {
            List<File> ret = new ArrayList<>();
            File file = new File(StringUtils.join(parent.getParentFolder(), File.separator, relativePath));
            if (file.exists())
                ret.add(file);
            else
                ret.addAll(Arrays.asList(new File(parent.getParentFolder()).listFiles((dir, name) -> {
                    return name.startsWith(relativePath);
                })));

            return ret;
        }

        public void delete(boolean deleteContents)
        {
            if (!file.exists())
                return;

            if (deleteContents)
                deleteContents();

            delete(file);
            parent.sync();
        }

        public boolean exists()
        {
            return file.exists();
        }
    }

    /**
     * The transaction data. It contains the two transaction log files,
     * the folder path and descriptor, the transaction id, type and
     * the final result, succeeded, which indicates if the transaction was
     * committed or aborted.
     *
     * We split the transaction data from the behavior because we need
     * the data to reconstruct any left-overs and clean them up, as well as work
     * out which files are temporary. So for these cases we don't want the full
     * transactional behavior, plus it's handy for the TransactionTidier.
     */
    final static class TransactionData implements AutoCloseable
    {
        private final OperationType opType;
        private final UUID id;
        private final File folder;
        private final TransactionFile[] files;
        private int folderDescriptor;

        static TransactionData make(File logFile)
        {
            Matcher matcher = TransactionFile.REGEX.matcher(logFile.getName());
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
            this.files = new TransactionFile[TransactionFile.Type.values().length];
            for (TransactionFile.Type t : TransactionFile.Type.values())
                this.files[t.idx] = new TransactionFile(t, this);

            this.folderDescriptor = CLibrary.tryOpenDirectory(folder.getPath());
        }

        public void close()
        {
            if (folderDescriptor > 0)
            {
                CLibrary.tryCloseFD(folderDescriptor);
                folderDescriptor = -1;
            }
        }

        void crossReference()
        {
            newLog().add(oldLog().file.getPath());
            oldLog().add(newLog().file.getPath());
        }

        void sync()
        {
            if (folderDescriptor > 0)
                CLibrary.trySync(folderDescriptor);
        }

        TransactionFile newLog()
        {
            return files[TransactionFile.Type.NEW.idx];
        }

        TransactionFile oldLog()
        {
            return files[TransactionFile.Type.OLD.idx];
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
                if (newLog().exists())
                    newLog().delete(true);
                else
                    oldLog().delete(true);
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

            if (newLog().exists())
                return newLog().getTrackedFiles();
            else
                return oldLog().getTrackedFiles();
        }

        String getFileName(TransactionFile.Type type)
        {
            String fileName = StringUtils.join(opType.fileName,
                                               TransactionFile.SEP,
                                               id.toString(),
                                               TransactionFile.SEP,
                                               type.txt,
                                               TransactionFile.EXT);
            return StringUtils.join(folder, File.separator, fileName);
        }

        String getParentFolder()
        {
            return folder.getParent();
        }

        static boolean isLogFile(String name)
        {
            return TransactionFile.REGEX.matcher(name).matches();
        }
    }

    private final Tracker tracker;
    private final TransactionData data;
    private final Ref<TransactionLogs> selfRef;
    private final Map<Descriptor, SSTableTidier> sstableTidiers = new HashMap<>();
    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue<>();
    private static final Blocker blocker = new Blocker();

    public TransactionLogs(OperationType opType, CFMetaData metadata)
    {
        this(opType, metadata, null);
    }

    public TransactionLogs(OperationType opType, CFMetaData metadata, Tracker tracker)
    {
        this(opType, new Directories(metadata), tracker);
    }

    public TransactionLogs(OperationType opType, Directories directories, Tracker tracker)
    {
        this(opType, directories.getDirectoryForNewSSTables(), tracker);
    }

    public TransactionLogs(OperationType opType, File folder, Tracker tracker)
    {
        this.tracker = tracker;
        this.data = new TransactionData(opType,
                                        Directories.getTransactionsDirectory(folder),
                                        UUIDGen.getTimeUUID());
        this.selfRef = new Ref<>(this, new TransactionTidier(data));

        data.crossReference();
        if (logger.isDebugEnabled())
            logger.debug("Created transaction logs with id {}", data.id);
    }

    /**
     * Mark this table as new, it must be safe to call this method multiple times.
     **/
    public TransactionLogs trackNew(SSTable table)
    {
        data.newLog().add(table);
        return this;
    }

    /**
     * Mark this table as old, it must be safe to call this method multiple times.
     **/
    public TransactionLogs trackOld(SSTable table)
    {
        data.oldLog().add(table);
        return this;
    }

    // stef: don't think this should be used anywhere except SSTableRewriter.switchWriter(),
    // where we find we haven't used the current one. In which case we can also just assert that it is present in newLog()
    public TransactionLogs untrack(SSTable table, boolean delete)
    {
        if (!data.newLog().remove(table, delete))
            data.oldLog().remove(table, delete);

        return this;
    }

    public OperationType getType()
    {
        return data.getType();
    }

    public UUID getId()
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

    /**
     * The transaction tidier.
     *
     * When the transaction reference is fully released we try to delete all the obsolete files
     * depending on the transaction result.
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

    /**
     * The SSTableReader tidier. When a reader is fully released and no longer referenced
     * by any one, we run this. It keeps a reference to the parent transaction and releases
     * it when done, so that the final transaction cleanup can run when all obsolete readers
     * are released.
     */
    private static class SSTableTidier implements Runnable
    {
        // must not retain a reference to the SSTableReader, else leak detection cannot kick in
        private final Descriptor desc;
        private final long sizeOnDisk;
        private final Tracker tracker;
        private final Ref<TransactionLogs> parentRef;

        public SSTableTidier(SSTableReader referent, TransactionLogs parent)
        {
            this.desc = referent.descriptor;
            // TODO: (before commit) make sure not run in critical commit() section, or run safely (perhaps stash file pointers and do in run())
            this.sizeOnDisk = referent.bytesOnDisk();
            this.tracker = parent.tracker;
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

                TransactionFile.delete(datafile);
                // let the remainder be cleaned up by delete
                SSTable.delete(desc, SSTable.discoverComponentsFor(desc));
            }
            catch (Throwable t)
            {
                logger.error("Failed deletion for {}, we'll retry after GC and on server restart", desc);
                failedDeletions.add(this);
                return;
            }

            tracker.cfstore.metric.totalDiskSpaceUsed.dec(sizeOnDisk);

            // release the referent to the parent so that the all transaction files can be released
            parentRef.release();
        }
    }

    public void released(Descriptor desc)
    {
        sstableTidiers.get(desc).run();
    }

    /**
     * Retry all deletions that failed the first time around (presumably b/c the sstable was still mmap'd.)
     * Useful because there are times when we know GC has been invoked; also exposed as an mbean.
     */
    public static void rescheduleFailedDeletions()
    {
        Runnable task;
        while ( null != (task = failedDeletions.poll()))
            TransactionLogs.executor.submit(task);
    }

    public static void waitForDeletions()
    {
        FBUtilities.waitOnFuture(TransactionLogs.executor.schedule(() -> {}, 0, TimeUnit.MILLISECONDS));
    }

    @VisibleForTesting
    public static void pauseDeletions(boolean stop)
    {
        blocker.block(stop);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced, make
     * sure to track it as an old reader so that the global release might delete
     * any files we were not able to delete.
     */
    public void obsoleted(SSTableReader reader)
    {
        trackOld(reader);

        assert !sstableTidiers.containsKey(reader.descriptor);

        // stef: think this is better here than in the tidier that is potentially run multiple times
        if (tracker != null)
            tracker.notifyDeleting(reader);

        sstableTidiers.put(reader.descriptor, new SSTableTidier(reader, this));
    }

    private Throwable complete(Throwable accumulate, boolean succeeded)
    {
        try
        {
            if (succeeded)
                data.newLog().delete(false);
            else
                data.oldLog().delete(false);

            selfRef.release();
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
        return complete(accumulate, true);
    }

    protected Throwable doAbort(Throwable accumulate)
    {
        return complete(accumulate, false);
    }

    protected void doPrepare() { }

    /**
     * Called on startup to scan existing folders for any unfinished leftovers of
     * operations that were ongoing when the process exited.
     *
     * We check if the new transaction file exists first, and if so we clean it up
     * along with its contents, which includes the old file, else if only the old file exists
     * it means the operation has completed and we only cleanup the old file with its contents.
     */
    public static void removeUnfinishedLeftovers(CFMetaData metadata)
    {
        Throwable accumulate = null;
        Set<UUID> ids = new HashSet<>();

        for (File dir : getFolders(metadata, null))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try (TransactionData data = TransactionData.make(log))
                {
                    // we need to check this because there are potentially 2 log files per operation
                    if (ids.contains(data.id))
                        continue;

                    ids.add(data.id);
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
    public static Set<File> getTemporaryFiles(CFMetaData metadata, File folder)
    {
        Set<File> ret = new HashSet<>();
        Set<UUID> ids = new HashSet<>();

        for (File dir : getFolders(metadata, folder))
        {
            File[] logs = dir.listFiles((dir1, name) -> {
                return TransactionData.isLogFile(name);
            });

            for (File log : logs)
            {
                try(TransactionData data = TransactionData.make(log))
                {
                    // we need to check this because there are potentially 2 log files per transaction
                    if (ids.contains(data.id))
                        continue;

                    ids.add(data.id);
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
    public static Set<File> getLogFiles(CFMetaData metadata)
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
