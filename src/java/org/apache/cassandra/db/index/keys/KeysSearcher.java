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
package org.apache.cassandra.db.index.keys;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.composites.Composite;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.db.index.*;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class KeysSearcher extends SecondaryIndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(KeysSearcher.class);

    public KeysSearcher(SecondaryIndexManager indexManager, Set<ByteBuffer> columns)
    {
        super(indexManager, columns);
    }

    @Override
    public List<Row> search(ExtendedFilter filter)
    {
        assert filter.getClause() != null && !filter.getClause().isEmpty();
        final IndexExpression primary = highestSelectivityPredicate(filter);
        final SecondaryIndex index = indexManager.getIndexForColumn(primary.column);
        // TODO: this should perhaps not open and maintain a writeOp for the full duration, but instead only *try* to delete stale entries, without blocking if there's no room
        // as it stands, we open a writeOp and keep it open for the duration to ensure that should this CF get flushed to make room we don't block the reclamation of any room  being made
        try (OpOrder.Group writeOp = baseCfs.keyspace.writeOrder.start(); 
             OpOrder.Group baseOp = baseCfs.readOrdering.start(); 
             OpOrder.Group indexOp = index.getIndexCfs().readOrdering.start())
        {
            return baseCfs.filter(getIndexedIterator(writeOp, filter, primary, index), filter);
        }
    }

    private ColumnFamilyStore.AbstractScanIterator getIndexedIterator(final OpOrder.Group writeOp, 
                                                                       final ExtendedFilter filter, 
                                                                       final IndexExpression primary, 
                                                                       final SecondaryIndex index)
    {

        // Start with the most-restrictive indexed clause, then apply remaining clauses
        // to each row matching that clause.
        // TODO: allow merge join instead of just one index + loop
        assert index != null;
        assert index.getIndexCfs() != null;
        
        if (logger.isDebugEnabled())
            logger.debug("Most-selective indexed predicate is {}",
                         ((AbstractSimplePerColumnSecondaryIndex) index).expressionString(primary));

        /*
         * XXX: If the range requested is a token range, we'll have to start at the beginning (and stop at the end) of
         * the indexed row unfortunately (which will be inefficient), because we have not way to intuit the small
         * possible key having a given token. A fix would be to actually store the token along the key in the
         * indexed row.
         */
        final AbstractBounds<RowPosition> range = filter.dataRange.keyRange();
        CellNameType type = index.getIndexCfs().getComparator();
        final Composite startKey = range.left instanceof DecoratedKey ? type.make(((DecoratedKey)range.left).getKey()) : Composites.EMPTY;
        final Composite endKey = range.right instanceof DecoratedKey ? type.make(((DecoratedKey)range.right).getKey()) : Composites.EMPTY;

        final int meanColumns = Math.max(index.getIndexCfs().getMeanColumns(), 1);
        // We shouldn't fetch only 1 row as this provides buggy paging in case the first row doesn't satisfy all clauses
        final int rowsPerQuery = Math.max(Math.min(filter.maxRows(), filter.maxColumns() / meanColumns), 2);
        
        final SliceQueryFilter columnFilter = new SliceQueryFilter(startKey, endKey, false, rowsPerQuery);
        final Range<RowPosition> keyRange = index.getIndexKeysFor(filter, primary);

        final ColumnFamilyStore.AbstractCellIterator cellIt = index.getIndexCfs().getCellIterator(keyRange, columnFilter, filter.timestamp);
        
        final CellName primaryColumn = baseCfs.getComparator().cellFromByteBuffer(primary.column);

        return new ColumnFamilyStore.AbstractScanIterator()
        {
            protected Row computeNext()
            {
                while (cellIt.hasNext())
                {
                    Cell cell = cellIt.next();
                    if (!cell.isLive(filter.timestamp))
                    {
                        logger.trace("skipping {}", cell.name());
                        continue;
                    }

                    DecoratedKey dk = baseCfs.partitioner.decorateKey(cell.name().toByteBuffer());
                    if (!range.right.isMinimum(baseCfs.partitioner) && range.right.compareTo(dk) < 0)
                    {
                        logger.trace("Reached end of assigned scan range");
                        return endOfData();
                    }
                    if (!range.contains(dk))
                    {
                        logger.trace("Skipping entry {} outside of assigned scan range", dk.getToken());
                        continue;
                    }
                    if (range instanceof Range && cell.name().equals(startKey))
                    {
                        logger.trace("Skipping start key as range excludes it");
                        continue;
                    }

                    logger.trace("Returning index hit for {}", dk);
                    ColumnFamily data = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, filter.columnFilter(cell.name().toByteBuffer()), filter.timestamp));
                    // While the column family we'll get in the end should contains the primary clause cell, the initialFilter may not have found it and can thus be null
                    if (data == null)
                        data = ArrayBackedSortedColumns.factory.create(baseCfs.metadata);

                    // as in CFS.filter - extend the filter to ensure we include the columns
                    // from the index expressions, just in case they weren't included in the initialFilter
                    IDiskAtomFilter extraFilter = filter.getExtraFilter(dk, data);
                    if (extraFilter != null)
                    {
                        ColumnFamily cf = baseCfs.getColumnFamily(new QueryFilter(dk, baseCfs.name, extraFilter, filter.timestamp));
                        if (cf != null)
                            data.addAll(cf);
                    }

                    if (((KeysIndex)index).isIndexEntryStale(cellIt.currentKey().getKey(), data, filter.timestamp))
                    {
                        // delete the index entry w/ its own timestamp
                        Cell dummyCell = new BufferCell(primaryColumn, cellIt.currentKey().getKey(), cell.timestamp());
                        ((PerColumnSecondaryIndex)index).delete(dk.getKey(), dummyCell, writeOp);
                        continue;
                    }
                    return new Row(dk, data);
                }
                
                return endOfData();
             }

            public void close() throws IOException 
            {
                cellIt.close();
            }
        };
    }
}
