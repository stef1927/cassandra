package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.io.sstable.IndexHelper;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.thrift.ThriftResultsMerger;
import org.apache.cassandra.utils.IMergeIterator;

/**
 * An unfiltered row iterator with a lower bound retrieved from either the global
 * sstable statistics or the row index lower bounds (if available in the cache).
 * Before initializing the sstable unfiltered row iterator, we return an empty row
 * with the clustering set to the lower bound. The empty row will be filtered out and
 * the result is that if we don't need to access this sstable, i.e. due to the LIMIT conditon,
 * then we will not. See CASSANDRA-8180 for examples of why this is useful.
 */
public class LowerBoundUnfilteredRowIterator extends LazilyInitializedUnfilteredRowIterator implements IMergeIterator.LowerBound<Unfiltered>
{
    private final SSTableReader sstable;
    private final ClusteringIndexSliceFilter filter;
    private final ColumnFilter selectedColumns;
    private final boolean isForThrift;
    private final int nowInSec;
    private Clustering globalLowerBound;
    private Clustering rowIndexLowerBound;

    public LowerBoundUnfilteredRowIterator(DecoratedKey partitionKey, SSTableReader sstable, ClusteringIndexSliceFilter filter, ColumnFilter selectedColumns, boolean isForThrift, int nowInSec)
    {
        super(partitionKey);
        this.sstable = sstable;
        this.filter = filter;
        this.selectedColumns = selectedColumns;
        this.isForThrift = isForThrift;
        this.nowInSec = nowInSec;

        setLowerBounds();

        assert globalLowerBound != null;
    }

    public Unfiltered lowerBound()
    {
        return ArrayBackedRow.emptyRow(rowIndexLowerBound == null ? globalLowerBound : rowIndexLowerBound);
    }

    @Override
    protected UnfilteredRowIterator initializeIterator()
    {
        sstable.incrementReadCount();

        @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
        UnfilteredRowIterator iter = filter.filter(sstable.iterator(partitionKey(), selectedColumns, filter.isReversed(), isForThrift));
        return isForThrift ? ThriftResultsMerger.maybeWrap(iter, nowInSec) : iter;
    }

    @Override
    public CFMetaData metadata()
    {
        return sstable.metadata;
    }

    @Override
    public boolean isReverseOrder()
    {
        return filter.isReversed();
    }

    @Override
    public PartitionColumns columns()
    {
        return selectedColumns.fetchedColumns();
    }

    @Override
    public RowStats stats()
    {
        return sstable.stats();
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        if (!sstable.hasTombstones())
            return DeletionTime.LIVE;

        DeletionTime ret = super.partitionLevelDeletion();
        //in case we now have a row index, let's update the
        //lower bound to something more accurate
        setLowerBounds();
        return ret;
    }

    private void setLowerBounds()
    {
        if (rowIndexLowerBound == null)
        {
            RowIndexEntry rowIndexEntry = sstable.getCachedPosition(partitionKey(), false);
            if (rowIndexEntry != null)
            {
                List<IndexHelper.IndexInfo> columns = rowIndexEntry.columnsIndex();
                if (columns.size() > 0)
                {
                    IndexHelper.IndexInfo column = columns.get(filter.isReversed() ? columns.size() - 1 : 0);
                    ClusteringPrefix lowerBoundPrefix = filter.isReversed() ? column.lastName : column.firstName;
                    rowIndexLowerBound = new Clustering(lowerBoundPrefix.getRawValues());
                }
            }
        }

        if (globalLowerBound == null)
        {
            final StatsMetadata m = sstable.getSSTableMetadata();
            List<ByteBuffer> vals = filter.isReversed() ? m.maxClusteringValues : m.minClusteringValues;
            globalLowerBound = new Clustering(vals.toArray(new ByteBuffer[vals.size()]));
        }
    }
}
