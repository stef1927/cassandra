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

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;

/**
 * {@code SSTableReaderListener} used to collect metrics about SSTable read access.
 */
public class SSTableReadMetricsCollector implements SSTableReadsListener
{
    /**
     * The number of SSTables that need to be merged. This counter is only updated for single partition queries
     * since this has been the behavior so far.
     */
    private int mergedSSTables;

    @Override
    public void skippingSSTable(SSTableReader sstable, Reason reason)
    {
    }

    @Override
    public void keyCacheHit(SSTableReader sstable)
    {
        sstable.incrementReadCount();
        mergedSSTables++;
    }

    @Override
    public void indexEntryFound(SSTableReader sstable, RowIndexEntry<?> indexEntry)
    {
        sstable.incrementReadCount();
        mergedSSTables++;
    }

    @Override
    public void scanning(SSTableReader sstable)
    {
        // SSTables are only scanned during partition range queries
        sstable.incrementReadCount();
    }

    /**
     * Returns the number of SSTables that need to be merged.
     * @return the number of SSTables that need to be merged.
     */
    public int getMergedSSTables()
    {
        return mergedSSTables;
    }
}
