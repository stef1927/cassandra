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

package org.apache.cassandra.db.batch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static junit.framework.Assert.assertNotNull;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;

public class BatchRemoveTest
{
    private static final String KEYSPACE = "BatchRemoveTest";
    private static final String CF_STANDARD = "Standard";

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE, CF_STANDARD, 1, BytesType.instance));
    }

    @Test
    public void testSerialization() throws IOException
    {
        UUID uuid = UUIDGen.getTimeUUID();

        BatchRemove request1 = new BatchRemove(uuid);
        assertEquals(uuid, request1.uuid);

        DataOutputBuffer out = new DataOutputBuffer();
        BatchRemove.serializer.serialize(request1, out, MessagingService.current_version);

        DataInputPlus dis = new DataInputBuffer(out.getData());
        BatchRemove request2 = BatchRemove.serializer.deserialize(dis, MessagingService.current_version);

        assertEquals(request1.uuid, request2.uuid);
    }

    @Test
    public void testRemoveBatch()
    {
        long initialAllBatches = BatchlogManager.instance.countAllBatches();
        CFMetaData cfm = Keyspace.open(KEYSPACE).getColumnFamilyStore(CF_STANDARD).metadata;

        int version = MessagingService.current_version;
        long timestamp = (System.currentTimeMillis() - DatabaseDescriptor.getWriteRpcTimeout() * 2) * 1000;
        UUID uuid = UUIDGen.getTimeUUID();

        // Add a batch with 10 mutations
        List<Mutation> mutations = new ArrayList<>(10);
        for (int j = 0; j < 10; j++)
        {
            mutations.add(new RowUpdateBuilder(cfm, FBUtilities.timestampMicros(), bytes(j))
                          .clustering("name" + j)
                          .add("val", "val" + j)
                          .build());
        }


        BatchStore batchStore = new BatchStore(uuid, timestamp).mutations(mutations);
        Mutation mutation = batchStore.getMutation(version);
        mutation.apply();
        Assert.assertEquals(initialAllBatches + 1, BatchlogManager.instance.countAllBatches());

        // Remove the batch
        BatchRemove request = new BatchRemove(uuid);
        mutation = request.getMutation();
        assertNotNull(mutation);
        mutation.apply();

        assertEquals(initialAllBatches, BatchlogManager.instance.countAllBatches());

        String query = String.format("SELECT count(*) FROM %s.%s where id = %s",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.BATCHES,
                                     uuid);
        assertEquals(0L, executeInternal(query).one().getLong("count"));
    }
}
