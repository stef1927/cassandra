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
import java.util.UUID;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;

public class BatchRemove
{
    public static final IVersionedSerializer<BatchRemove> serializer = new Serializer();

    public final UUID uuid;

    public BatchRemove(UUID uuid)
    {
        this.uuid = uuid;
    }

    public MessageOut<BatchRemove> createMessage()
    {
        return createMessage(MessagingService.Verb.BATCH_REMOVE);
    }

    public MessageOut<BatchRemove> createMessage(MessagingService.Verb verb)
    {
        return new MessageOut<>(verb, this, serializer);
    }

    public Mutation getMutation()
    {
        return getMutation(uuid);
    }

    public static Mutation getMutation(UUID uuid)
    {
        return new Mutation(PartitionUpdate.fullPartitionDelete(SystemKeyspace.Batches,
                                                                UUIDType.instance.decompose(uuid),
                                                                FBUtilities.timestampMicros(),
                                                                FBUtilities.nowInSeconds()));
    }

    public static class Serializer implements IVersionedSerializer<BatchRemove>
    {
        public void serialize(BatchRemove request, DataOutputPlus out, int version) throws IOException
        {
            UUIDSerializer.serializer.serialize(request.uuid, out, version);
        }

        public BatchRemove deserialize(DataInputPlus in, int version) throws IOException
        {
            return new BatchRemove(UUIDSerializer.serializer.deserialize(in, version));
        }

        public long serializedSize(BatchRemove request, int version)
        {
            return UUIDSerializer.serializer.serializedSize(request.uuid, version);
        }
    }
}
