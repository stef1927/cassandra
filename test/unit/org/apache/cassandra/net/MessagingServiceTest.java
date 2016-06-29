/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MessagingServiceTest
{
    private static volatile MessagingService messagingService;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setBackPressureStrategy(new ParameterizedClass("org.apache.cassandra.net.MessagingServiceTest$MockBackPressureStrategy", Collections.emptyMap()));
        messagingService = MessagingService.test();
    }

    @Before
    public void before()
    {
        messagingService.destroyConnectionPool(InetAddress.getLoopbackAddress());
    }
    
    @Test
    public void testDroppedMessages()
    {
        MessagingService.Verb verb = MessagingService.Verb.READ;

        for (int i = 0; i < 5000; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        List<String> logs = messagingService.getDroppedMessagesLogs();
        assertEquals(1, logs.size());
        assertEquals("READ messages were dropped in last 5000 ms: 2500 for internal timeout and 2500 for cross node timeout", logs.get(0));
        assertEquals(5000, (int)messagingService.getDroppedMessages().get(verb.toString()));

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals(0, logs.size());

        for (int i = 0; i < 2500; i++)
            messagingService.incrementDroppedMessages(verb, i % 2 == 0);

        logs = messagingService.getDroppedMessagesLogs();
        assertEquals("READ messages were dropped in last 5000 ms: 1250 for internal timeout and 1250 for cross node timeout", logs.get(0));
        assertEquals(7500, (int)messagingService.getDroppedMessages().get(verb.toString()));
    }

    @Test
    public void testUpdatesBackPressureStateWhenEnabledAndWithSupportedCallback()
    {
        BackPressureState backPressureState = messagingService.getConnectionPool(InetAddress.getLoopbackAddress()).getBackPressureState();
        IAsyncCallback bpCallback = new BackPressureCallback();
        IAsyncCallback noCallback = new NoBackPressureCallback();

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureState(InetAddress.getLoopbackAddress(), noCallback);
        assertEquals(0.0, backPressureState.incomingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.updateBackPressureState(InetAddress.getLoopbackAddress(), bpCallback);
        assertEquals(0.0, backPressureState.incomingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.updateBackPressureState(InetAddress.getLoopbackAddress(), bpCallback);
        assertEquals(1.0, backPressureState.incomingRate.get(TimeUnit.SECONDS), 0.0);
    }

    @Test
    public void testAppliesBackPressureWhenEnabled()
    {
        BackPressureState backPressureState = messagingService.getConnectionPool(InetAddress.getLoopbackAddress()).getBackPressureState();
        
        DatabaseDescriptor.setBackPressureEnabled(false);
        messagingService.applyBackPressure(InetAddress.getLoopbackAddress());
        assertFalse(MockBackPressureStrategy.applied);
        assertEquals(0.0, backPressureState.outgoingRate.get(TimeUnit.SECONDS), 0.0);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(InetAddress.getLoopbackAddress());
        assertTrue(MockBackPressureStrategy.applied);
        assertEquals(1.0, backPressureState.outgoingRate.get(TimeUnit.SECONDS), 0.0);
    }
    
    @Test
    public void testDoesntIncrementOutgoingRateWhenOverloaded()
    {
        BackPressureState backPressureState = messagingService.getConnectionPool(InetAddress.getLoopbackAddress()).getBackPressureState();
        
        backPressureState.overload.set(true);

        DatabaseDescriptor.setBackPressureEnabled(true);
        messagingService.applyBackPressure(InetAddress.getLoopbackAddress());
        assertTrue(MockBackPressureStrategy.applied);
        assertEquals(0.0, backPressureState.outgoingRate.get(TimeUnit.SECONDS), 0.0);
    }
    
    public static class MockBackPressureStrategy implements BackPressureStrategy
    {
        public static volatile boolean applied = false;

        public MockBackPressureStrategy(Map<String, Object> args)
        {
        }

        @Override
        public void apply(BackPressureState state)
        {
            applied = true;
        }
    }

    private static class BackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return true;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }

    private static class NoBackPressureCallback implements IAsyncCallback
    {
        @Override
        public boolean supportsBackPressure()
        {
            return false;
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }

        @Override
        public void response(MessageIn msg)
        {
            throw new UnsupportedOperationException("Not supported.");
        }
    }
}
