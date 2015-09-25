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

package org.apache.cassandra.db.monitoring;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MonitoringTaskTest
{
    private static final long timeout = 100;
    private static final long MAX_SPIN_TIME_NANOS = TimeUnit.SECONDS.toNanos(5);

    public static final long CHECK_INTERVAL_MS = 600000; // long enough so that it won't check unless told to do so
    public static final int REPORT_INTERVAL_MS = 0; // report every time we check
    public static final int MAX_TIMEDOUT_OPERATIONS = -1; // unlimited

    @BeforeClass
    public static void setup()
    {
        MonitoringTask.instance = MonitoringTask.make(CHECK_INTERVAL_MS, REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
    }

    private static final class TestMonitor extends MonitorableImpl
    {
        private final String name;

        TestMonitor(String name, ConstructionTime constructionTime, long timeout)
        {
            this.name = name;
            setMonitoringTime(constructionTime, timeout);
        }

        public String name()
        {
            return name;
        }

        @Override
        public String toString()
        {
            return name();
        }
    }

    private static void waitForOperationsToComplete(Monitorable... operations) throws InterruptedException
    {
        waitForOperationsToComplete(Arrays.asList(operations));
    }

    private static void waitForOperationsToComplete(List<Monitorable> operations) throws InterruptedException
    {
        long timeout = operations.stream().map(Monitorable::timeout).reduce(0L, Long::max);
        Thread.sleep(timeout * 2 + ApproximateTime.precision());

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            long numInProgress = operations.stream().filter(Monitorable::inProgress).count();
            if (numInProgress == 0)
                return;

            Thread.yield();
        }
    }

    @Test
    public void testAbort() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test abort", new ConstructionTime(System.currentTimeMillis()), timeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.aborted());
        assertFalse(operation.completed());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testAbortIdemPotent() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test abort", new ConstructionTime(System.currentTimeMillis()), timeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.abort());

        assertTrue(operation.aborted());
        assertFalse(operation.completed());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testAbortCrossNode() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test for cross node", new ConstructionTime(System.currentTimeMillis(), true), timeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.aborted());
        assertFalse(operation.completed());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testComplete() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test complete", new ConstructionTime(System.currentTimeMillis()), timeout);
        operation.complete();
        waitForOperationsToComplete(operation);

        assertFalse(operation.aborted());
        assertTrue(operation.completed());
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testCompleteIdemPotent() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test complete", new ConstructionTime(System.currentTimeMillis()), timeout);
        operation.complete();
        waitForOperationsToComplete(operation);

        assertTrue(operation.complete());

        assertFalse(operation.aborted());
        assertTrue(operation.completed());
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testReport() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test report", new ConstructionTime(System.currentTimeMillis()), timeout);
        waitForOperationsToComplete(operation);

        assertTrue(operation.aborted());
        assertFalse(operation.completed());
        MonitoringTask.instance.checkFailedOperations(ApproximateTime.currentTimeMillis(), true);
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testRealScheduling() throws InterruptedException
    {
        MonitoringTask.instance = MonitoringTask.make(10, 100, -1);
        try
        {
            Monitorable operation = new TestMonitor("Test report", new ConstructionTime(System.currentTimeMillis()), timeout);
            waitForOperationsToComplete(operation);

            assertTrue(operation.aborted());
            assertFalse(operation.completed());

            Thread.sleep(ApproximateTime.precision() + 500);
            assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
        }
        finally
        {
            MonitoringTask.instance = MonitoringTask.make(CHECK_INTERVAL_MS, REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
        }
    }

    @Test
    public void testMultipleThreads() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test thread " + Thread.currentThread().getName(),
                                                            new ConstructionTime(System.currentTimeMillis()),
                                                            timeout);
                    operations.add(operation);
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToComplete(operations);
        assertEquals(threadCount, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testZeroMaxTimedoutOperations() throws InterruptedException
    {
        doTestMaxTimedoutOperations(0, 1, 0, false);
    }

    @Test
    public void testMaxTimedoutOperationsExceeded() throws InterruptedException
    {
        doTestMaxTimedoutOperations(5, 10, 6, false);
    }

    @Test
    public void testMaxTimedoutOperationsExceededInTotal() throws InterruptedException
    {
        doTestMaxTimedoutOperations(5, 10, 6, true);
    }

    private void doTestMaxTimedoutOperations(int maxTimedoutOperations,
                                                    int numThreads,
                                                    int numExpectedOperations,
                                                    boolean partiallyAggregate) throws InterruptedException
    {
        MonitoringTask.instance = MonitoringTask.make(CHECK_INTERVAL_MS, REPORT_INTERVAL_MS, maxTimedoutOperations);
        try
        {
            final int threadCount = numThreads;
            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            final CountDownLatch finished = new CountDownLatch(threadCount);

            for (int i = 0; i < threadCount; i++)
            {
                final String operationName = "Operation " + Integer.toString(i+1);
                final int numTimes = i + 1;
                executorService.submit(() -> {
                    try
                    {
                        for (int j = 0; j < numTimes; j++)
                        {
                            Monitorable operation = new TestMonitor(operationName,
                                                                    new ConstructionTime(System.currentTimeMillis()),
                                                                    timeout);
                            waitForOperationsToComplete(operation);

                            if (partiallyAggregate)
                            {
                                synchronized (this)
                                { // this is normally called only by the monitoring thread
                                  // so we simulate this with a synchronized in the tests
                                    MonitoringTask.instance.aggregateFailedOperations();
                                }
                            }
                        }
                    }
                    catch (InterruptedException e)
                    {
                        e.printStackTrace();
                        fail("Unexpected exception");
                    }
                    finally
                    {
                        finished.countDown();
                    }
                });
            }

            finished.await();
            assertEquals(0, executorService.shutdownNow().size());

            List<String> failedOperations = MonitoringTask.instance.getFailedOperations();
            assertEquals(numExpectedOperations, failedOperations.size());
            if (numExpectedOperations > 0)
                assertEquals("...", failedOperations.get(numExpectedOperations - 1));
        }
        finally
        {
            MonitoringTask.instance = MonitoringTask.make(CHECK_INTERVAL_MS, REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);
        }
    }

    @Test
    public void testMultipleThreadsSameName() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test testMultipleThreadsSameName",
                                                            new ConstructionTime(System.currentTimeMillis()),
                                                            timeout);
                    operations.add(operation);
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToComplete(operations);
        //MonitoringTask.instance.checkFailedOperations(ApproximateTime.currentTimeMillis());
        assertEquals(1, MonitoringTask.instance.getFailedOperations().size());
    }

    @Test
    public void testMultipleThreadsNoFailedOps() throws InterruptedException
    {
        final int threadCount = 50;
        final List<Monitorable> operations = new ArrayList<>(threadCount);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            executorService.submit(() -> {
                try
                {
                    Monitorable operation = new TestMonitor("Test thread " + Thread.currentThread().getName(),
                                                            new ConstructionTime(System.currentTimeMillis()),
                                                            timeout);
                    operations.add(operation);
                    operation.complete();
                }
                finally
                {
                    finished.countDown();
                }
            });
        }

        finished.await();
        assertEquals(0, executorService.shutdownNow().size());

        waitForOperationsToComplete(operations);
        assertEquals(0, MonitoringTask.instance.getFailedOperations().size());
    }
}
