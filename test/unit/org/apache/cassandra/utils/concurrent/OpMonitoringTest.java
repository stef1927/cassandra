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

package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.ScheduledFuture;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpMonitoringTest
{
    @Test
    public void testAbort() throws InterruptedException
    {
        long now = System.currentTimeMillis();
        long timeout = 1000;

        OpState state = new OpState();
        OpMonitoring.schedule(now, timeout, state);

        Thread.sleep(timeout + 1);
        assertTrue(state.aborted());
    }

    @Test
    public void testCancel() throws InterruptedException
    {
        long now = System.currentTimeMillis();
        long timeout = 1000;

        OpState state = new OpState();
        ScheduledFuture<?> monitor = OpMonitoring.schedule(now, timeout, state);

        monitor.cancel(false);

        Thread.sleep(timeout + 1);
        assertFalse(state.aborted());
    }
}
