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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.google.common.primitives.Ints;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.UUIDSerializer;

public class BatchStore
{
    public static final IVersionedSerializer<BatchStore> serializer = new Serializer();

    public final UUID uuid;
    public final long timeMicros;
    public final Collection<Mutation> mutations;
    public final Collection<ByteBuffer> serializedMutations;

    public BatchStore(UUID uuid, long timeMicros)
    {
        this.uuid = uuid;
        this.timeMicros = timeMicros;
        this.mutations = new ArrayList<>();
        this.serializedMutations = new ArrayList<>();
    }

    public BatchStore mutations(Collection<Mutation> mutations)
    {
        this.mutations.addAll(mutations);
        return this;
    }

    public BatchStore serializedMutations(Collection<ByteBuffer> serializedMutations)
    {
        this.serializedMutations.addAll(serializedMutations);
        return this;
    }

    public MessageOut<BatchStore> createMessage()
    {
        return createMessage(MessagingService.Verb.BATCH_STORE);
    }

    public MessageOut<BatchStore> createMessage(MessagingService.Verb verb)
    {
        return new MessageOut<>(verb, this, serializer);
    }

    public Mutation getMutation(int version)
    {
        RowUpdateBuilder builder = new RowUpdateBuilder(SystemKeyspace.Batches, timeMicros, uuid)
                                   .clustering()
                                   .add("version", version);

        if (serializedMutations.isEmpty())
            serializeMutations(version); // local case

        for (ByteBuffer serializedMutation : serializedMutations)
            builder.addListEntry("mutations",serializedMutation);

        return builder.build();
    }

    private void serializeMutations(int version)
    {
        for (Mutation mutation : mutations)
        {
            try (DataOutputBuffer buf = new DataOutputBuffer())
            {
                Mutation.serializer.serialize(mutation, buf, version);
                serializedMutations.add(buf.buffer());
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Serializer implements IVersionedSerializer<BatchStore>
    {
        public void serialize(BatchStore request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.uuid, out, version);
            out.writeLong(request.timeMicros);

            out.writeInt(request.mutations.size());
            for (Mutation mutation : request.mutations)
            {
                out.writeLong(Mutation.serializer.serializedSize(mutation, version));
                Mutation.serializer.serialize(mutation, out, version);
            }
        }

        public BatchStore deserialize(DataInputPlus in, int version) throws IOException
        {
            UUID uuid = UUIDSerializer.serializer.deserialize(in, version);
            long timeMicros = in.readLong();

            int numMutations = in.readInt();
            List<ByteBuffer> serializedMutations = new ArrayList<>(numMutations);
            for (int i = 0; i < numMutations; i++)
            {
                long size = in.readLong();
                ByteBuffer buffer = ByteBuffer.allocate(Ints.checkedCast(size));
                in.readFully(buffer.array());
                serializedMutations.add(buffer);
            }
            return new BatchStore(uuid, timeMicros).serializedMutations(serializedMutations);
        }

        public long serializedSize(BatchStore request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.uuid, version) +
                   TypeSizes.sizeof(request.timeMicros) +
                   serializedSize(request.mutations, version);
        }

        private long serializedSize(Collection<Mutation> mutations, int version)
        {
            long ret = TypeSizes.sizeof(mutations.size());
            for (Mutation mutation : mutations)
            {
                long size = Mutation.serializer.serializedSize(mutation, version);
                ret += (TypeSizes.sizeof(size) + size);
            }
            return ret;
        }

    }
}
