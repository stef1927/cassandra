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
package org.apache.cassandra.batchlog;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
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
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class LegacyBatchlogMigrator
{
    private static final Logger logger = LoggerFactory.getLogger(LegacyBatchlogMigrator.class);

    private LegacyBatchlogMigrator()
    {
        // static class
    }

    @SuppressWarnings("deprecation")
    public static void migrate()
    {
        int convertedBatches = 0;
        String query = String.format("SELECT id, data, written_at, version FROM %s.%s",
                                     SystemKeyspace.NAME,
                                     SystemKeyspace.LEGACY_BATCHLOG);

        ColumnFamilyStore store = Keyspace.open(SystemKeyspace.NAME).getColumnFamilyStore(SystemKeyspace.LEGACY_BATCHLOG);
        int pageSize = BatchlogManager.calculatePageSize(store);

        UntypedResultSet rows = QueryProcessor.executeInternalWithPaging(query, pageSize);
        for (UntypedResultSet.Row row : rows)
        {
            if (apply(row, convertedBatches, true))
                convertedBatches++;
        }

        if (convertedBatches > 0)
            Keyspace.openAndGetStore(SystemKeyspace.LegacyBatchlog).truncateBlocking();
    }

    @SuppressWarnings("deprecation")
    public static boolean isLegacyBatchlogMutation(Mutation mutation)
    {
        return mutation.getKeyspaceName().equals(SystemKeyspace.NAME)
            && mutation.getPartitionUpdate(SystemKeyspace.LegacyBatchlog.cfId) != null;
    }

    @SuppressWarnings("deprecation")
    public static void handleLegacyMutation(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdate(SystemKeyspace.LegacyBatchlog.cfId);
        logger.debug("Applying legacy batchlog mutation {}", update);
        update.forEach(row -> apply(UntypedResultSet.Row.fromInternalRow(update.metadata(), update.partitionKey(), row), -1, false));
    }

    private static boolean apply(UntypedResultSet.Row row, long counter, boolean migrating)
    {
        UUID id = row.getUUID("id");
        long timestamp = id.version() == 1 ? UUIDGen.unixTimestamp(id) : row.getLong("written_at");
        int version = row.has("version") ? row.getInt("version") : MessagingService.VERSION_12;

        if (migrating)
            logger.info("Converting mutation at {}", timestamp);
        else
            logger.debug("Converting mutation at {}", timestamp);

        try (DataInputBuffer in = new DataInputBuffer(row.getBytes("data"), false))
        {
            if (id.version() != 1)
                id = UUIDGen.getTimeUUID(timestamp, counter >= 0 ? counter : nanoSince(id, timestamp));

            int numMutations = in.readInt();
            List<Mutation> mutations = new ArrayList<>(numMutations);
            for (int i = 0; i < numMutations; i++)
                mutations.add(Mutation.serializer.deserialize(in, version));

            BatchlogManager.store(Batch.createLocal(id, timestamp * 1000, mutations));
            return true;
        }
        catch (Throwable t)
        {
            logger.error("Failed to convert mutation {} at timestamp {}", id, timestamp, t);
            return false;
        }
    }

    private static synchronized long nanoSince(UUID id, long timestamp)
    {
        String query = String.format("SELECT id FROM %s.%s WHERE id = ?", SystemKeyspace.NAME, SystemKeyspace.BATCHES);
        long nanos = 0;
        while (nanos < 10000)
        {
            UntypedResultSet rows = QueryProcessor.executeOnceInternal(query, UUIDGen.getTimeUUID(timestamp, nanos));
            if (rows == null || rows.isEmpty())
                return nanos;

            nanos++;
        }

        logger.error("Failed to find an empty id for legacy mutation {} with timestamp {}", id, timestamp);
        return 0;
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

        Batch batch = Batch.createLocal(uuid, FBUtilities.timestampMicros(), mutations);

        for (InetAddress target : endpoints)
        {
            logger.debug("Sending legacy batchlog store request {} to {} for {} mutations", uuid, target, mutations.size());

            int targetVersion = MessagingService.instance().getVersion(target);
            MessagingService.instance().sendRR(getStoreMutation(batch, targetVersion).createMessage(MessagingService.Verb.MUTATION),
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
            logger.debug("Sending legacy batchlog remove request {} to {}", uuid, target);
            MessagingService.instance().sendRR(mutation.createMessage(MessagingService.Verb.MUTATION), target, handler, false);
        }
    }

    static void store(Batch batch, int version)
    {
        getStoreMutation(batch, version).apply();
    }

    @SuppressWarnings("deprecation")
    static Mutation getStoreMutation(Batch batch, int version)
    {
        return new RowUpdateBuilder(SystemKeyspace.LegacyBatchlog, batch.creationTime, batch.id)
               .clustering()
               .add("written_at", new Date(batch.creationTime / 1000))
               .add("data", getSerializedMutations(version, batch.decodedMutations))
               .add("version", version)
               .build();
    }

    @SuppressWarnings("deprecation")
    private static Mutation getRemoveMutation(UUID uuid)
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
