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
package org.apache.cassandra.io.sstable.format;

import org.apache.cassandra.db.RowIndexEntry;

/**
 * Listener for receiving notifications associated with reading SSTables.
 */
public interface SSTableReadsListener
{
    /**
     * The reasons for skipping an SSTable
     */
    enum Reason
    {
        BLOOM_FILTER,
        MIN_MAX_KEYS,
        PARTITION_INDEX_LOOKUP,
        INDEX_ENTRY_NOT_FOUND
    }

    /**
     * Listener that does nothing.
     */
    static final SSTableReadsListener NOOP_LISTENER = new SSTableReadsListener()
    {
        @Override
        public void skippingSSTable(SSTableReader sstable, Reason reason)
        {
        }

        @Override
        public void keyCacheHit(SSTableReader sstable)
        {
        }

        @Override
        public void indexEntryFound(SSTableReader sstable, RowIndexEntry<?> indexEntry)
        {
        }

        @Override
        public void scanning(SSTableReader sstable)
        {
        }
    };

    /**
     * Handles notification that the specified SSTable has been skipped during a single partition query.
     *
     * @param sstable the SSTable reader
     * @param reason the reason for which the SSTable has been skipped
     */
    void skippingSSTable(SSTableReader sstable, Reason reason);

    /**
     * Notifies that the key cache has been hit for the specified SSTable during a single partition query.
     * @param sstable the SSTable reader
     */
    void keyCacheHit(SSTableReader sstable);

    /**
     * Handles notification that the index entry has been found during a single partition query.
     *
     * @param sstable the SSTable reader
     * @param indexEntry the index entry
     */
    void indexEntryFound(SSTableReader sstable, RowIndexEntry<?> indexEntry);

    /**
     * Handles notification that the specified SSTable is being scanned during a partition range query.
     *
     * @param sstable the SSTable reader of the table being scanned.
     */
    void scanning(SSTableReader sstable);
}
