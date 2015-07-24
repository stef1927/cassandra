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
package org.apache.cassandra.db;

import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.rows.LowerBoundUnfilteredRowIterator;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.memory.HeapAllocator;

/**
 * General interface for storage-engine read queries.
 */
public class SinglePartitionSliceCommand extends SinglePartitionReadCommand<ClusteringIndexSliceFilter>
{
    public SinglePartitionSliceCommand(boolean isDigest,
                                       boolean isForThrift,
                                       CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexSliceFilter clusteringIndexFilter)
    {
        super(isDigest, isForThrift, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    public SinglePartitionSliceCommand(CFMetaData metadata,
                                       int nowInSec,
                                       ColumnFilter columnFilter,
                                       RowFilter rowFilter,
                                       DataLimits limits,
                                       DecoratedKey partitionKey,
                                       ClusteringIndexSliceFilter clusteringIndexFilter)
    {
        this(false, false, metadata, nowInSec, columnFilter, rowFilter, limits, partitionKey, clusteringIndexFilter);
    }

    /**
     * Creates a new single partition slice command for the provided single slice.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slice the slice of rows to query.
     *
     * @return a newly created read command that queries {@code slice} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slice slice)
    {
        return create(metadata, nowInSec, key, Slices.with(metadata.comparator, slice));
    }

    /**
     * Creates a new single partition slice command for the provided slices.
     *
     * @param metadata the table to query.
     * @param nowInSec the time in seconds to use are "now" for this query.
     * @param key the partition key for the partition to query.
     * @param slices the slices of rows to query.
     *
     * @return a newly created read command that queries the {@code slices} in {@code key}. The returned query will
     * query every columns for the table (without limit or row filtering) and be in forward order.
     */
    public static SinglePartitionReadCommand create(CFMetaData metadata, int nowInSec, DecoratedKey key, Slices slices)
    {
        ClusteringIndexSliceFilter filter = new ClusteringIndexSliceFilter(slices, false);
        return new SinglePartitionSliceCommand(metadata, nowInSec, ColumnFilter.all(metadata), RowFilter.NONE, DataLimits.NONE, key, filter);
    }

    public SinglePartitionSliceCommand copy()
    {
        return new SinglePartitionSliceCommand(isDigestQuery(), isForThrift(), metadata(), nowInSec(), columnFilter(), rowFilter(), limits(), partitionKey(), clusteringIndexFilter());
    }

    private final static class QueryMemtableAndDiskStatus
    {
        public final TableMetrics metric;
        public int sstablesIterated;
        public int nonIntersectingSSTables;
        public int includedDueToTombstones;
        public long mostRecentPartitionTombstone;
        public long minTimestamp;

        public QueryMemtableAndDiskStatus(ColumnFamilyStore cfs)
        {
            this.metric = cfs.metric;
            this.sstablesIterated = 0;
            this.nonIntersectingSSTables = 0;
            this.includedDueToTombstones = 0;
            this.mostRecentPartitionTombstone = Long.MIN_VALUE;
            this.minTimestamp = Long.MAX_VALUE;
        }
    }
    protected UnfilteredRowIterator queryMemtableAndDiskInternal(ColumnFamilyStore cfs, boolean copyOnHeap)
    {
        final QueryMemtableAndDiskStatus status = new QueryMemtableAndDiskStatus((cfs));
        Tracing.trace("Acquiring sstable references");
        final ColumnFamilyStore.ViewFragment view = cfs.select(cfs.viewFilter(partitionKey()));

        List<UnfilteredRowIterator> iterators = new ArrayList<>(Iterables.size(view.memtables) + view.sstables.size());
        ClusteringIndexSliceFilter filter = clusteringIndexFilter();

        try
        {
            for (Memtable memtable : view.memtables)
            {
                Partition partition = memtable.getPartition(partitionKey());
                if (partition == null)
                    continue;

                @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
                UnfilteredRowIterator iter = filter.getUnfilteredRowIterator(columnFilter(), partition);
                @SuppressWarnings("resource") // same as above
                UnfilteredRowIterator maybeCopied = copyOnHeap ? UnfilteredRowIterators.cloningIterator(iter, HeapAllocator.instance) : iter;
                iterators.add(isForThrift() ? ThriftResultsMerger.maybeWrap(maybeCopied, nowInSec()) : maybeCopied);
            }

            /*
             * We can't eliminate full sstables based on the timestamp of what we've already read like
             * in collectTimeOrderedData, but we still want to eliminate sstable whose maxTimestamp < mostRecentTombstone
             * we've read. We still rely on the sstable ordering by maxTimestamp since if
             *   maxTimestamp_s1 > maxTimestamp_s0,
             * we're guaranteed that s1 cannot have a row tombstone such that
             *   timestamp(tombstone) > maxTimestamp_s0
             * since we necessarily have
             *   timestamp(tombstone) <= maxTimestamp_s1
             * In other words, iterating in maxTimestamp order allow to do our mostRecentPartitionTombstone elimination
             * in one pass, and minimize the number of sstables for which we read a partition tombstone.
             */
            Collections.sort(view.sstables, SSTableReader.maxTimestampComparator);

            List<SSTableReader> skippedSSTablesWithTombstones = null;

            for (SSTableReader sstable : view.sstables)
            {
                status.minTimestamp = Math.min(status.minTimestamp, sstable.getMinTimestamp());
                // if we've already seen a partition tombstone with a timestamp greater
                // than the most recent update to this sstable, we can skip it
                if (sstable.getMaxTimestamp() < status.mostRecentPartitionTombstone)
                    break;

                if (!filter.shouldInclude(sstable))
                {
                    status.nonIntersectingSSTables++;
                    if (sstable.hasTombstones())
                    { // if sstable has tombstones we need to check after one pass if it can be safely skipped
                        if (skippedSSTablesWithTombstones == null)
                            skippedSSTablesWithTombstones = new ArrayList<>();
                        skippedSSTablesWithTombstones.add(sstable);
                    }
                    continue;
                }

                UnfilteredRowIterator iter = getSSTableLazyIterator(sstable, status);
                iterators.add(iter);
                status.mostRecentPartitionTombstone = Math.max(status.mostRecentPartitionTombstone, iter.partitionLevelDeletion().markedForDeleteAt());
            }

            // Check for partition tombstones in the skipped sstables
            if (skippedSSTablesWithTombstones != null)
            {
                for (SSTableReader sstable : skippedSSTablesWithTombstones)
                {
                    if (sstable.getMaxTimestamp() <= status.minTimestamp)
                        continue;

                    UnfilteredRowIterator iter = getSSTableLazyIterator(sstable, status);
                    if (iter.partitionLevelDeletion().markedForDeleteAt() > status.minTimestamp)
                    {
                        iterators.add(iter);
                        status.includedDueToTombstones++;
                    }
                    else
                    {
                        iter.close();
                    }
                }
            }
            if (Tracing.isTracing())
                Tracing.trace("Skipped {}/{} non-slice-intersecting sstables, included {} due to tombstones",
                              status.nonIntersectingSSTables, view.sstables.size(), status.includedDueToTombstones);

            if (iterators.isEmpty())
                return UnfilteredRowIterators.emptyIterator(cfs.metadata, partitionKey(), filter.isReversed());

            Tracing.trace("Merging data from memtables and {} sstables", status.sstablesIterated);

            @SuppressWarnings("resource") //  Closed through the closing of the result of that method.
            final UnfilteredRowIterator merged = UnfilteredRowIterators.merge(iterators, nowInSec());
            return new WrappingUnfilteredRowIterator(merged)
            {
                private boolean hasData = false;

                @Override
                public boolean isEmpty()
                {
                    return merged.partitionLevelDeletion().isLive()
                           && !hasData
                           && merged.staticRow().isEmpty();
                }

                @Override
                public boolean hasNext()
                {
                    if (!hasData)
                    {
                        hasData = super.hasNext();
                        return hasData;
                    }
                    return super.hasNext();
                }

                public void close()
                {
                    super.close();

                    cfs.metric.updateSSTableIterated(status.sstablesIterated);

                    if (!isEmpty())
                    {
                        DecoratedKey key = merged.partitionKey();
                        cfs.metric.samplers.get(TableMetrics.Sampler.READS).addSample(key.getKey(), key.hashCode(), 1);
                    }

                    Tracing.trace("Accessed {} sstables", new Object[]{ status.sstablesIterated });
                }
            };
        }
        catch (RuntimeException | Error e)
        {
            try
            {
                FBUtilities.closeAll(iterators);
            }
            catch (Exception suppressed)
            {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }


    private UnfilteredRowIterator getSSTableLazyIterator(final SSTableReader sstable, final QueryMemtableAndDiskStatus status)
    {
        final ClusteringIndexSliceFilter filter =  clusteringIndexFilter();
        final StatsMetadata m = sstable.getSSTableMetadata();

        if (m.minClusteringValues.isEmpty() || filter.isReversed() && m.maxClusteringValues.isEmpty())
        {
            status.sstablesIterated++;
            sstable.incrementReadCount();

            @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
            UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), columnFilter(), filter.isReversed(), isForThrift()));
            return isForThrift() ? ThriftResultsMerger.maybeWrap(iter, nowInSec()) : iter;
        }

        return new LowerBoundUnfilteredRowIterator(partitionKey(), sstable, filter, columnFilter(), isForThrift(), nowInSec())
        {
            @Override
            protected UnfilteredRowIterator initializeIterator()
            {
                status.sstablesIterated++;
                return super.initializeIterator();
            }
        };
    }

}
