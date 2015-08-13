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
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class LegacyBatchMigrator
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyBatchMigrator.class);

    private LegacyBatchMigrator()
    {
        // static class
    }

    @SuppressWarnings("deprecation")
    public static void convertBatchEntries()
    {
        logger.debug("Started convertBatchEntries");

        String query = String.format("SELECT id, data, written_at, version FROM %s.%s",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.LEGACY_BATCHLOG);
        UntypedResultSet batches = executeInternalWithPaging(query, BatchlogManager.DEFAULT_PAGE_SIZE);
        int convertedBatches = 0;
        for (UntypedResultSet.Row row : batches)
        {
            UUID id = row.getUUID("id");
            long timestamp = row.getLong("written_at");
            int version = row.has("version") ? row.getInt("version") : MessagingService.VERSION_12;

            if (logger.isDebugEnabled())
                logger.debug("Converting mutation at {}", timestamp);

            try
            {
                UUID newId = id;
                if (id.version() != 1 || timestamp != UUIDGen.unixTimestamp(id))
                    newId = UUIDGen.getTimeUUID(timestamp, convertedBatches);

                DataInputPlus in = new DataInputBuffer(row.getBytes("data"), false);
                int numMutations = in.readInt();
                List<Mutation> mutations = new ArrayList(numMutations);
                for (int i = 0; i < numMutations; i++)
                    mutations.add(Mutation.serializer.deserialize(in, version));

                Mutation addRow = new BatchStore(newId, FBUtilities.timestampMicros())
                                  .mutations(mutations)
                                  .getMutation(version);

                addRow.apply();
                ++convertedBatches;
            }
            catch (Throwable t)
            {
                logger.error("Failed to convert mutation {} at timestamp {}", id, timestamp, t);
            }
        }

        if (convertedBatches > 0)
            Keyspace.openAndGetStore(SystemKeyspace.LegacyBatchlog).truncateBlocking();

        // cleanup will be called after replay
        logger.debug("Finished convertBatchEntries");
    }

    @VisibleForTesting
    @SuppressWarnings("deprecation")
    public static int countAllBatches()
    {
        String query = String.format("SELECT count(*) FROM %s.%s", SystemKeyspace.NAME, SystemKeyspace.LEGACY_BATCHLOG);
        UntypedResultSet results = executeInternal(query);
        if (results.isEmpty())
            return 0;
        return (int) results.one().getLong("count");
    }

    public static AbstractWriteResponseHandler<?> syncWriteToBatchlog(Collection<Mutation> mutations, Collection<InetAddress> endpoints, UUID uuid)
    throws WriteTimeoutException, WriteFailureException
    {
        AbstractWriteResponseHandler<IMutation> handler = new WriteResponseHandler<>(endpoints,
                                                                                     Collections.<InetAddress>emptyList(),
                                                                                     ConsistencyLevel.ONE,
                                                                                     Keyspace.open(SystemKeyspace.NAME),
                                                                                     null,
                                                                                     WriteType.BATCH_LOG);

        BatchStore batchStore = new BatchStore(uuid, FBUtilities.timestampMicros()).mutations(mutations);

        for (InetAddress target : endpoints)
        {
            if (logger.isDebugEnabled())
                logger.debug("Sending legacy batchlog store request {} to {} for {} mutations", uuid, target, mutations.size());

            int targetVersion = MessagingService.instance().getVersion(target);
            MessagingService.instance().sendRR(getStoreMutation(targetVersion, batchStore).createMessage(MessagingService.Verb.MUTATION),
                                               target,
                                               handler,
                                               false);

        }

        return handler;
    }

    public static void asyncRemoveFromBatchlog(Collection<InetAddress> endpoints, UUID uuid)
    {
        AbstractWriteResponseHandler<IMutation> handler = new WriteResponseHandler<>(endpoints,
                                                                                     Collections.<InetAddress>emptyList(),
                                                                                     ConsistencyLevel.ANY,
                                                                                     Keyspace.open(SystemKeyspace.NAME),
                                                                                     null,
                                                                                     WriteType.SIMPLE);
        Mutation mutation = getRemoveMutation(uuid);

        for (InetAddress target : endpoints)
        {
            if (logger.isDebugEnabled())
                logger.debug("Sending legacy batchlog remove request {} to {}", uuid, target);

            MessagingService.instance().sendRR(mutation.createMessage(MessagingService.Verb.MUTATION), target, handler, false);
        }
    }

    @SuppressWarnings("deprecation")
    static Mutation getStoreMutation(int version, BatchStore cmd)
    {
        return new RowUpdateBuilder(SystemKeyspace.LegacyBatchlog, cmd.timeMicros, cmd.uuid)
               .clustering()
               .add("written_at", new Date(cmd.timeMicros / 1000))
               .add("data", getSerializedMutations(version, cmd.mutations))
               .add("version", version)
               .build();
    }

    @SuppressWarnings("deprecation")
    static Mutation getRemoveMutation(UUID uuid)
    {
        return new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.LegacyBatchlog,
                                                                UUIDType.instance.decompose(uuid),
                                                                FBUtilities.timestampMicros(),
                                                                FBUtilities.nowInSeconds()));
    }

    private static ByteBuffer getSerializedMutations(int version, Collection<Mutation> mutations)
    {
        try (DataOutputBuffer buf = new DataOutputBuffer())
        {
            buf.writeInt(mutations.size());
            for (Mutation mutation : mutations)
                Mutation.serializer.serialize(mutation, buf, version);
            return buf.buffer();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
}
