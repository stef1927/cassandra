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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OpStateTest
{
    @Test
    public void testInit()
    {
        OpState state = new OpState();
        assertFalse(state.aborted());
        assertFalse(state.completed());
    }

    @Test
    public void testComplete()
    {
        OpState state = new OpState();
        assertTrue(state.complete());

        assertFalse(state.aborted());
        assertTrue(state.completed());

        assertFalse(state.abort());
        assertTrue(state.complete());
    }


    @Test
    public void testAbort()
    {
        OpState state = new OpState();
        assertTrue(state.abort());

        assertTrue(state.aborted());
        assertFalse(state.completed());

        assertTrue(state.abort());
        assertFalse(state.complete());
    }

    @Test
    public void testRaces() throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        final int nTests = 10000;
        final CountDownLatch finished = new CountDownLatch(nTests);

        for (int i = 0; i < nTests; i++)
        {
           final OpState state = new OpState();

            executorService.submit(() -> {
                state.complete();
                assertTrue(state.aborted());
                assertFalse(state.completed());
                finished.countDown();
            });

            executorService.submit(() -> {
                state.abort();
                assertFalse(state.aborted());
                assertTrue(state.completed());
                finished.countDown();
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());
    }
}
