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
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Runnables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LogRecord.Type;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.db.Directories.OnTxnErr;
import static org.apache.cassandra.db.Directories.FileType;

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
class LogTransaction extends Transactional.AbstractTransactional implements Transactional
{
    private static final Logger logger = LoggerFactory.getLogger(LogTransaction.class);

    /**
     * If the format of the lines in the transaction log is wrong or the checksum
     * does not match, then we throw this exception.
     */
    public static final class CorruptTransactionLogException extends RuntimeException
    {
        public final LogFile file;

        public CorruptTransactionLogException(String message, LogFile file)
        {
            super(message);
            this.file = file;
        }
    }

    private final Tracker tracker;
    private final LogData data;
    private final Ref<LogTransaction> selfRef;
    // Deleting sstables is tricky because the mmapping might not have been finalized yet,
    // and delete will fail (on Windows) until it is (we only force the unmapping on SUN VMs).
    // Additionally, we need to make sure to delete the data file first, so on restart the others
    // will be recognized as GCable.
    private static final Queue<Runnable> failedDeletions = new ConcurrentLinkedQueue<>();

    LogTransaction(OperationType opType, CFMetaData metadata)
    {
        this(opType, metadata, null);
    }

    LogTransaction(OperationType opType, CFMetaData metadata, Tracker tracker)
    {
        this(opType, new Directories(metadata), tracker);
    }

    LogTransaction(OperationType opType, Directories directories, Tracker tracker)
    {
        this(opType, directories.getDirectoryForNewSSTables(), tracker);
    }

    LogTransaction(OperationType opType, File folder, Tracker tracker)
    {
        this.tracker = tracker;
        this.data = new LogData(opType,
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
        if (!data.file.add(Type.ADD, table))
            throw new IllegalStateException(table + " is already tracked as new");
    }

    /**
     * Stop tracking a reader as new.
     */
    void untrackNew(SSTable table)
    {
        data.file.remove(Type.ADD, table);
    }

    /**
     * Schedule a reader for deletion as soon as it is fully unreferenced.
     */
    SSTableTidier obsoleted(SSTableReader reader)
    {
        if (data.file.contains(Type.ADD, reader))
        {
            if (data.file.contains(Type.REMOVE, reader))
                throw new IllegalArgumentException();

            return new SSTableTidier(reader, true, this);
        }

        if (!data.file.add(Type.REMOVE, reader))
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
    LogData getData()
    {
        return data;
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
        private final LogData data;

        TransactionTidier(LogData data)
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
        private final Ref<LogTransaction> parentRef;

        public SSTableTidier(SSTableReader referent, boolean wasNew, LogTransaction parent)
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
            File[] logs = dir.listFiles(LogData::isLogFile);

            for (File log : logs)
            {
                try (LogData data = LogData.make(log))
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
            list(Files.newDirectoryStream(folder))
            .stream()
            .filter((f) -> !LogData.isLogFile(f))
            .forEach((f) -> files.put(f, FileType.FINAL));

            // Since many file systems are not atomic, we cannot be sure we have listed a consistent disk state
            // (Linux would permit this, but for simplicity we keep our behaviour the same across platforms)
            // so we must be careful to list txn log files AFTER every other file since these files are deleted last,
            // after all other files are removed
            list(Files.newDirectoryStream(folder, '*' + LogFile.EXT))
            .stream()
            .filter(LogData::isLogFile)
            .forEach(this::classifyFiles);

            // Finally we apply the user filter before returning our result
            return files.entrySet().stream()
                      .filter((e) -> filter.apply(e.getKey(), e.getValue()))
                      .map(Map.Entry::getKey)
                      .collect(Collectors.toList());
        }

        List<File> list(DirectoryStream<Path> stream) throws IOException
        {
            try
            {
                return StreamSupport.stream(stream.spliterator(), false)
                                    .map(Path::toFile)
                                    .filter((f) -> !f.isDirectory())
                                    .collect(Collectors.toList());
            }
            finally
            {
                stream.close();
            }
        }

        /**
         * We read txn log files, if we fail we throw only if the user has specified
         * OnTxnErr.THROW, else we log an error and apply the txn log anyway
         */
        void classifyFiles(File txnFile)
        {
            try (LogData txn = LogData.make(txnFile))
            {
                readTxnLog(txn);
                classifyFiles(txn);
                files.put(txnFile, FileType.TXN_LOG);
            }
        }

        void readTxnLog(LogData txn)
        {
            Throwable err = txn.check(txn.readLogFile(null));
            if (err != null)
            {
                if (onTxnErr == OnTxnErr.THROW)
                    throw err;

                logger.error("Failed to read temporary files of txn {}: {}", txn, err.getMessage());
            }
        }

        void classifyFiles(LogData txn)
        {
            LogFile txnFile = txn.file;

            Map<LogRecord, Set<File>> oldFiles = txnFile.getFilesOfType(files.navigableKeySet(), Type.REMOVE);
            Map<LogRecord, Set<File>> newFiles = txnFile.getFilesOfType(files.navigableKeySet(), Type.ADD);

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
        private boolean allFilesPresent(LogFile txnFile, Map<LogRecord, Set<File>> oldFiles, Map<LogRecord, Set<File>> newFiles)
        {
            LogRecord lastRecord = txnFile.getLastRecord();
            return !Stream.concat(oldFiles.entrySet().stream(),
                                  newFiles.entrySet().stream()
                                          .filter((e) -> e.getKey() != lastRecord))
                          .filter((e) -> e.getKey().numFiles > e.getValue().size())
                          .findFirst().isPresent();
        }

        private void filter(LogFile txnFile, Collection<Set<File>> oldFiles, Collection<Set<File>> newFiles)
        {
            Collection<Set<File>> temporary = txnFile.committed() ? oldFiles : newFiles;
            temporary.stream()
                     .flatMap(Set::stream)
                     .forEach((f) -> this.files.put(f, FileType.TEMPORARY));
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
    static Set<File> listFiles(CFMetaData metadata, File folder, boolean forceNonAtomicListing, FileType ... types)
    {
        Collection<FileType> match = Arrays.asList(types);
        List<File> directories = new Directories(metadata).getCFDirectories();
        directories.add(folder);
        return directories.stream()
                          .flatMap((dir) -> new FileLister(dir.toPath(),
                                                           (file, type) -> match.contains(type),
                                                           OnTxnErr.IGNORE,
                                                           forceNonAtomicListing).list().stream())
                          .collect(Collectors.toSet());
    }
}
