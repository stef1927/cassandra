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

package org.apache.cassandra.gms;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;

import static org.junit.Assert.assertTrue;

public class EndpointStateTest
{
    public volatile VersionedValue.VersionedValueFactory valueFactory =
        new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

    @Test
    public void testMultiThreadedConsistency() throws InterruptedException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadedConsistency();
    }

    private void innerTestMultiThreadedConsistency() throws InterruptedException
    {
        final Token token = ByteOrderedPartitioner.instance.getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);
        final AtomicInteger numFailures = new AtomicInteger();

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            public void run()
            {
                for (int i = 0; i < 50; i++)
                {
                    Map<ApplicationState, VersionedValue> values = new HashMap<>();
                    for (Map.Entry<ApplicationState, VersionedValue> entry : state.getApplicationStateMap().entrySet())
                    {
                        values.put(entry.getKey(), entry.getValue());
                    }

                    if (values.containsKey(ApplicationState.STATUS) && !values.containsKey(ApplicationState.TOKENS))
                    {
                        numFailures.incrementAndGet();
                        System.out.println(String.format("Failed: %s", values));
                    }
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue(numFailures.get() == 0);
    }
}
