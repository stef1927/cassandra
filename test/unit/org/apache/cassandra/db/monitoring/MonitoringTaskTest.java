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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MonitoringTaskTest
{
    private static final MonitorableThreadLocal monitoringTask = new MonitorableThreadLocal();
    private static final long timeout = 10;
    private static final long MAX_SPIN_TIME_NANOS = TimeUnit.SECONDS.toNanos(5);

    @BeforeClass
    public static void setup()
    {
        // Let's speed things up a bit
        DatabaseDescriptor.setMonitoringCheckIntervalMillis(5);

        // This disables real-time reporting so that we can check the failed operations directly
        DatabaseDescriptor.setMonitoringReportIntervalMillis(-1);

        // Make sure that by default we report all operations that timed out
        DatabaseDescriptor.setMonitoringMaxTimedoutOperations(-1);
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
        Thread.sleep(timeout * 2);

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            long numInProgress = operations.stream().map(Monitorable::state).filter(MonitoringStateRef::inProgress).count();
            if (numInProgress == 0)
                return;

            Thread.yield();
        }
    }

    @Test
    public void testAbort() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test abort", new ConstructionTime(System.currentTimeMillis()), timeout);
        monitoringTask.update(operation);

        waitForOperationsToComplete(operation);

        assertTrue(operation.state().aborted());
        assertFalse(operation.state().completed());
        assertEquals(1, MonitoringTask.logFailedOperations().size());
    }

    @Test
    public void testAbortCrossNode() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test for cross node", new ConstructionTime(System.currentTimeMillis(), true), timeout);
        monitoringTask.update(operation);

        waitForOperationsToComplete(operation);

        assertTrue(operation.state().aborted());
        assertFalse(operation.state().completed());
        assertEquals(1, MonitoringTask.logFailedOperations().size());
    }

    @Test
    public void testComplete() throws InterruptedException
    {
        Monitorable operation = new TestMonitor("Test complete", new ConstructionTime(System.currentTimeMillis()), timeout);
        monitoringTask.update(operation);
        operation.state().complete();

        waitForOperationsToComplete(operation);

        assertFalse(operation.state().aborted());
        assertTrue(operation.state().completed());
        assertEquals(0, MonitoringTask.logFailedOperations().size());
    }

    @Test
    public void testReport() throws InterruptedException
    {
        int oldReportInterval = DatabaseDescriptor.getMonitoringReportIntervalMillis();

        //This ensures we report every time we check, so we exercise the code path without extra waiting time
        DatabaseDescriptor.setMonitoringReportIntervalMillis(DatabaseDescriptor.getMonitoringCheckIntervalMillis());

        try
        {
            Monitorable operation = new TestMonitor("Test report", new ConstructionTime(System.currentTimeMillis()), timeout);
            monitoringTask.update(operation);

            waitForOperationsToComplete(operation);

            assertTrue(operation.state().aborted());
            assertFalse(operation.state().completed());
            assertEquals(0, MonitoringTask.logFailedOperations().size());
        }
        finally
        {
            DatabaseDescriptor.setMonitoringReportIntervalMillis(oldReportInterval);
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
                    monitoringTask.update(operation);
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
        assertEquals(threadCount, MonitoringTask.logFailedOperations().size());
    }

    @Test
    public void testMaxTimedoutOperations() throws InterruptedException
    {
        int oldMaxTimedoutOperations = DatabaseDescriptor.getMonitoringMaxTimedoutOperations();
        DatabaseDescriptor.setMonitoringMaxTimedoutOperations(5);

        try
        {
            final int threadCount = 10;
            final List<Monitorable> operations = new ArrayList<>(threadCount);
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
                            operations.add(operation);
                            monitoringTask.update(operation);
                            waitForOperationsToComplete(operation);
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

            waitForOperationsToComplete(operations);

            List<String> failedOperations = MonitoringTask.logFailedOperations();
            assertEquals(6, failedOperations.size()); // 5 operations plus the ...
            assertTrue(failedOperations.get(0).startsWith("Operation 10"));
            assertTrue(failedOperations.get(1).startsWith("Operation 9"));
            assertTrue(failedOperations.get(2).startsWith("Operation 8"));
            assertTrue(failedOperations.get(3).startsWith("Operation 7"));
            assertTrue(failedOperations.get(4).startsWith("Operation 6"));
            assertEquals("...", failedOperations.get(5));
        }
        finally
        {
            DatabaseDescriptor.setMonitoringMaxTimedoutOperations(oldMaxTimedoutOperations);
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
                    monitoringTask.update(operation);
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
        assertEquals(1, MonitoringTask.logFailedOperations().size());
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
                    monitoringTask.update(operation);
                    operation.state().complete();
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
        assertEquals(0, MonitoringTask.logFailedOperations().size());
    }
}
