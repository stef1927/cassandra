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
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.*;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedSegmentedFile;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.AlwaysPresentFilter;
import org.apache.cassandra.utils.FBUtilities;

public class TransactionLogsTest extends SchemaLoader
{
    private static final String KEYSPACE = "TransactionLogsTest";
    private static final String REWRITE_FINISHED_CF = "RewriteFinished";
    private static final String REWRITE_ABORTED_CF = "RewriteAborted";
    private static final String FLUSH_CF = "Flush";

    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, REWRITE_FINISHED_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, REWRITE_ABORTED_CF),
                                    SchemaLoader.standardCFMD(KEYSPACE, FLUSH_CF));
    }

    @Test
    public void testCommit() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);
        assertNotNull(transactionLogs.getId());
        Assert.assertEquals(OperationType.COMPACTION, transactionLogs.getType());

        transactionLogs.trackOld(sstableOld);
        transactionLogs.trackNew(sstableNew);

        transactionLogs.obsoleted(sstableOld);
        transactionLogs.finish();

        sstableOld.selfRef().release();
        TransactionLogs.waitForDeletions();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
    }

    @Test
    public void testAbort() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstableOld);
        transactionLogs.trackNew(sstableNew);

        transactionLogs.abort();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();
    }

    @Test
    public void testCancel() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstableOld);
        transactionLogs.trackNew(sstableNew);

        transactionLogs.untrack(sstableOld, false);
        transactionLogs.untrack(sstableNew, true);

        transactionLogs.finish();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
        sstableOld.selfRef().release();
    }

    @Test
    public void testCommitSameDesc() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld1 = sstable(cfs, 0, 128);
        SSTableReader sstableOld2 = sstable(cfs, 0, 256);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstableOld1);
        transactionLogs.trackNew(sstableNew);
        transactionLogs.trackOld(sstableOld2);

        transactionLogs.obsoleted(sstableOld1, sstableOld2);
        transactionLogs.finish();

        sstableOld1.selfRef().release();
        sstableOld2.selfRef().release();
        TransactionLogs.waitForDeletions();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstableNew.selfRef().release();
    }

    @Test
    public void testCommitOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstable);
        transactionLogs.finish();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstable.selfRef().release();
    }

    @Test
    public void testCommitOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstable);
        transactionLogs.finish();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>());
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstable.selfRef().release();
    }

    @Test
    public void testAbortOnlyNew() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackNew(sstable);
        transactionLogs.abort();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>());
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstable.selfRef().release();
    }

    @Test
    public void testAbortOnlyOld() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable = sstable(cfs, 0, 128);

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstable);
        transactionLogs.abort();

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstable.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());
        assertEquals(0, TransactionLogs.getLogFiles(cfs.metadata).size());

        sstable.selfRef().release();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_newLogFound() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a failed transaction (new log file NOT deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstableOld);
        transactionLogs.trackNew(sstableNew);

        sstableNew.selfRef().release();

        Set<File> tmpFiles = new HashSet<>(TransactionLogs.getLogFiles(cfs.metadata));
        for (String p : sstableNew.getAllFilePaths())
            tmpFiles.add(new File(p));

        Assert.assertEquals(tmpFiles, TransactionLogs.getTemporaryFiles(cfs.metadata, sstableNew.descriptor.directory));

        // normally called at startup
        TransactionLogs.removeUnfinishedLeftovers(cfs.metadata);

        // sstable should not have been removed because the new log was found
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableOld.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());

        sstableOld.selfRef().release();
        transactionLogs.close();
    }

    @Test
    public void testRemoveUnfinishedLeftovers_oldLogFound() throws Throwable
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstableOld = sstable(cfs, 0, 128);
        SSTableReader sstableNew = sstable(cfs, 1, 128);

        // simulate tracking sstables with a committed transaction (new log file deleted)
        TransactionLogs transactionLogs = new TransactionLogs(OperationType.COMPACTION, cfs.metadata);
        assertNotNull(transactionLogs);

        transactionLogs.trackOld(sstableOld);
        transactionLogs.trackNew(sstableNew);

        transactionLogs.getData().newLog().delete(false);

        sstableOld.selfRef().release();

        Set<File> tmpFiles = new HashSet<>(TransactionLogs.getLogFiles(cfs.metadata));
        for (String p : sstableOld.getAllFilePaths())
            tmpFiles.add(new File(p));

        Assert.assertEquals(tmpFiles, TransactionLogs.getTemporaryFiles(cfs.metadata, sstableOld.descriptor.directory));

        // normally called at startup
        TransactionLogs.removeUnfinishedLeftovers(cfs.metadata);

        // sstable should have been removed because there was no new log.
        Directories directories = new Directories(cfs.metadata);
        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(1, sstables.size());

        assertFiles(transactionLogs.getDataFolder(), new HashSet<>(sstableNew.getAllFilePaths()));
        assertFiles(transactionLogs.getLogsFolder(), Collections.<String>emptySet());

        sstableNew.selfRef().release();
        transactionLogs.close();
    }

    @Test
    public void testGetTemporaryFiles() throws IOException
    {
        ColumnFamilyStore cfs = MockSchema.newCFS(KEYSPACE);
        SSTableReader sstable1 = sstable(cfs, 0, 128);

        File dataFolder = sstable1.descriptor.directory;

        Set<File> tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        TransactionLogs transactionLogs = new TransactionLogs(OperationType.WRITE, cfs.metadata);
        Directories directories = new Directories(cfs.metadata);

        File[] beforeSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());

        SSTableReader sstable2 = sstable(cfs, 1, 128);
        transactionLogs.trackNew(sstable2);

        Map<Descriptor, Set<Component>> sstables = directories.sstableLister().list();
        assertEquals(2, sstables.size());

        File[] afterSecondSSTable = dataFolder.listFiles(pathname -> !pathname.isDirectory());
        int numNewFiles = afterSecondSSTable.length - beforeSecondSSTable.length;
        assertTrue(numNewFiles == sstable2.getAllFilePaths().size());

        tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(numNewFiles + 2, tmpFiles.size()); //the extra files are the transaction log files

        File ssTable2DataFile = new File(sstable2.descriptor.filenameFor(Component.DATA));
        File ssTable2IndexFile = new File(sstable2.descriptor.filenameFor(Component.PRIMARY_INDEX));

        assertTrue(tmpFiles.contains(ssTable2DataFile));
        assertTrue(tmpFiles.contains(ssTable2IndexFile));

        List<File> files = directories.sstableLister().listFiles();
        List<File> filesNoTmp = directories.sstableLister().skipTemporary(true).listFiles();
        assertNotNull(files);
        assertNotNull(filesNoTmp);

        assertTrue(files.contains(ssTable2DataFile));
        assertTrue(files.contains(ssTable2IndexFile));

        assertFalse(filesNoTmp.contains(ssTable2DataFile));
        assertFalse(filesNoTmp.contains(ssTable2IndexFile));

        transactionLogs.finish();

        //Now it should be empty since the transaction has finished
        tmpFiles = TransactionLogs.getTemporaryFiles(cfs.metadata, dataFolder);
        assertNotNull(tmpFiles);
        assertEquals(0, tmpFiles.size());

        filesNoTmp = directories.sstableLister().skipTemporary(true).listFiles();
        assertNotNull(filesNoTmp);
        assertTrue(filesNoTmp.contains(ssTable2DataFile));
        assertTrue(filesNoTmp.contains(ssTable2IndexFile));

        sstable1.selfRef().release();
        sstable2.selfRef().release();
    }

    public static SSTableReader sstable(ColumnFamilyStore cfs, int generation, int size) throws IOException
    {
        Directories dir = new Directories(cfs.metadata);
        Descriptor descriptor = new Descriptor(dir.getDirectoryForNewSSTables(), cfs.keyspace.getName(), cfs.getColumnFamilyName(), generation);
        Set<Component> components = ImmutableSet.of(Component.DATA, Component.PRIMARY_INDEX, Component.FILTER, Component.TOC);
        for (Component component : components)
        {
            File file = new File(descriptor.filenameFor(component));
            file.createNewFile();
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw"))
            {
                raf.setLength(size);
            }
        }

        SegmentedFile dFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.DATA))), 0);
        SegmentedFile iFile = new BufferedSegmentedFile(new ChannelProxy(new File(descriptor.filenameFor(Component.PRIMARY_INDEX))), 0);

        SerializationHeader header = SerializationHeader.make(cfs.metadata, Collections.EMPTY_LIST);
        StatsMetadata metadata = (StatsMetadata) new MetadataCollector(cfs.metadata.comparator)
                                                 .finalizeMetadata(Murmur3Partitioner.instance.getClass().getCanonicalName(), 0.01f, -1, header)
                                                 .get(MetadataType.STATS);
        SSTableReader reader = SSTableReader.internalOpen(descriptor,
                                                          components,
                                                          cfs.metadata,
                                                          Murmur3Partitioner.instance,
                                                          dFile,
                                                          iFile,
                                                          MockSchema.indexSummary.sharedCopy(),
                                                          new AlwaysPresentFilter(),
                                                          1L,
                                                          metadata,
                                                          SSTableReader.OpenReason.NORMAL,
                                                          header);
        reader.first = reader.last = MockSchema.readerBounds(generation);
        return reader;
    }

    // Following tests simulate real compactions and flushing with SSTableRewriter, ColumnFamilyStore, etc

    @Test
    public void testRewriteFinished() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(REWRITE_FINISHED_CF);

        SSTableReader oldSSTable = getSSTable(cfs, 1);
        LifecycleTransaction txn = cfs.getTracker().tryModify(oldSSTable, OperationType.COMPACTION);
        SSTableReader newSSTable = replaceSSTable(cfs, txn, false);
        TransactionLogs.waitForDeletions();

        assertFiles(txn.logs().getDataFolder(), new HashSet<>(newSSTable.getAllFilePaths()));
        assertFiles(txn.logs().getLogsFolder(), Collections.<String>emptySet());
    }

    @Test
    public void testRewriteAborted() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(REWRITE_ABORTED_CF);

        SSTableReader oldSSTable = getSSTable(cfs, 1);
        LifecycleTransaction txn = cfs.getTracker().tryModify(oldSSTable, OperationType.COMPACTION);

        replaceSSTable(cfs, txn, true);
        TransactionLogs.waitForDeletions();

        assertFiles(txn.logs().getDataFolder(), new HashSet<>(oldSSTable.getAllFilePaths()));
        assertFiles(txn.logs().getLogsFolder(), Collections.<String>emptySet());
    }

    @Test
    public void testFlush() throws IOException
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(FLUSH_CF);

        SSTableReader ssTableReader = getSSTable(cfs, 100);

        String dataFolder = cfs.getSSTables().iterator().next().descriptor.directory.getPath();
        String transactionLogsFolder = StringUtils.join(dataFolder, File.separator, Directories.TRANSACTIONS_SUBDIR);

        assertTrue(new File(transactionLogsFolder).exists());
        assertFiles(transactionLogsFolder, Collections.<String>emptySet());

        assertFiles(dataFolder, new HashSet<>(ssTableReader.getAllFilePaths()));
    }

    private SSTableReader getSSTable(ColumnFamilyStore cfs, int numPartitions) throws IOException
    {
        addContentAndFlush(cfs, numPartitions);

        Set<SSTableReader> sstables = new HashSet<>(cfs.getSSTables());
        assertEquals(1, sstables.size());
        return sstables.iterator().next();
    }

    private void addContentAndFlush(ColumnFamilyStore cfs, int numPartitions) throws IOException
    {
        cfs.truncateBlocking();

        String schema = "CREATE TABLE %s.%s (key ascii, name ascii, val ascii, val1 ascii, PRIMARY KEY (key, name))";
        String query = "INSERT INTO %s.%s (key, name, val) VALUES (?, ?, ?)";

        try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                       .withPartitioner(StorageService.getPartitioner())
                                                       .forTable(String.format(schema, cfs.keyspace.getName(), cfs.name))
                                                       .using(String.format(query, cfs.keyspace.getName(), cfs.name))
                                                       .build())
        {
            for (int j = 0; j < numPartitions; j ++)
                writer.addRow(String.format("key%d", j), "col1", "0");
        }

        cfs.forceBlockingFlush();
    }

    private SSTableReader replaceSSTable(ColumnFamilyStore cfs, LifecycleTransaction txn, boolean fail)
    {
        List<SSTableReader> newsstables = null;
        int nowInSec = FBUtilities.nowInSeconds();
        try (CompactionController controller = new CompactionController(cfs, txn.originals(), cfs.gcBefore(FBUtilities.nowInSeconds())))
        {
            try (SSTableRewriter rewriter = new SSTableRewriter(cfs, txn, 1000, false);
                 AbstractCompactionStrategy.ScannerList scanners = cfs.getCompactionStrategyManager().getScanners(txn.originals());
                 CompactionIterator ci = new CompactionIterator(txn.opType(), scanners.scanners, controller, nowInSec, txn.opId())
            )
            {
                long lastCheckObsoletion = System.nanoTime();
                File directory = txn.originals().iterator().next().descriptor.directory;
                Descriptor desc = Descriptor.fromFilename(cfs.getSSTablePath(directory));
                CFMetaData metadata = Schema.instance.getCFMetaData(desc);
                rewriter.switchWriter(SSTableWriter.create(metadata,
                                                           desc,
                                                           0,
                                                           0,
                                                           0,
                                                           DatabaseDescriptor.getPartitioner(),
                                                           SerializationHeader.make(cfs.metadata, txn.originals()),
                                                           txn.logs()));
                while (ci.hasNext())
                {
                    rewriter.append(ci.next());

                    if (System.nanoTime() - lastCheckObsoletion > TimeUnit.MINUTES.toNanos(1L))
                    {
                        controller.maybeRefreshOverlaps();
                        lastCheckObsoletion = System.nanoTime();
                    }
                }

                if (!fail)
                    newsstables = rewriter.finish();
                else
                    rewriter.abort();
            }
        }

        assertTrue(fail || newsstables != null);

        if (newsstables != null)
        {
            Assert.assertEquals(1, newsstables.size());
            return newsstables.iterator().next();
        }

        return null;
    }

    private void assertFiles(String dirPath, Set<String> expectedFiles)
    {
        File dir = new File(dirPath);
        for (File file : dir.listFiles())
        {
            if (file.isDirectory())
                continue;

            String filePath = file.getPath();
            assertTrue(filePath, expectedFiles.contains(filePath));
            expectedFiles.remove(filePath);
        }

        assertTrue(expectedFiles.isEmpty());
    }
}
