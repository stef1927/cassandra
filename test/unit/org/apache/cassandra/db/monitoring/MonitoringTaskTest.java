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

import org.junit.Test;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MonitoringTaskTest
{
    private static final MonitorableThreadLocal monitoringTask = new MonitorableThreadLocal();
    private static final long timeout = MonitoringTask.CHECK_DELAY_MILLIS * 2;
    private static final long MAX_SPIN_TIME_NANOS = TimeUnit.SECONDS.toNanos(30);

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
        Thread.sleep(timeout);

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            long numInProgress = operations.stream().map(Monitorable::state).filter(MonitoringStateRef::inProgress).count();
            if (numInProgress == 0)
                return;
        }
    }

    private static void waitForReportToComplete() throws InterruptedException
    {
        Thread.sleep(MonitoringTask.REPORT_DELAY_MILLIS);

        long start = System.nanoTime();
        while(System.nanoTime() - start <= MAX_SPIN_TIME_NANOS)
        {
            if (MonitoringTask.numFailedOperations() == 0)
                return;
        }
    }

    @Test
    public void testAbort() throws InterruptedException
    {
        try
        {
            Monitorable operation = new TestMonitor("Test abort", new ConstructionTime(System.currentTimeMillis()), timeout);
            monitoringTask.update(operation);

            waitForOperationsToComplete(operation);

            assertTrue(operation.state().aborted());
            assertFalse(operation.state().completed());
        }
        finally
        {
            monitoringTask.reset();
        }
    }

    @Test
    public void testAbortCrossNode() throws InterruptedException
    {
        try
        {
            Monitorable operation = new TestMonitor("Test for cross node", new ConstructionTime(System.currentTimeMillis(), true), timeout);
            monitoringTask.update(operation);

            waitForOperationsToComplete(operation);

            assertTrue(operation.state().aborted());
            assertFalse(operation.state().completed());
        }
        finally
        {
            monitoringTask.reset();
        }
    }

    @Test
    public void testComplete() throws InterruptedException
    {
        try
        {
            Monitorable operation = new TestMonitor("Test complete", new ConstructionTime(System.currentTimeMillis()), timeout);
            monitoringTask.update(operation);
            operation.state().complete();

            waitForOperationsToComplete(operation);

            assertFalse(operation.state().aborted());
            assertTrue(operation.state().completed());
        }
        finally
        {
            monitoringTask.reset();
        }
    }

    @Test
    public void testReset() throws InterruptedException
    {
        try
        {
            monitoringTask.update(new TestMonitor("Test reset", new ConstructionTime(System.currentTimeMillis()), timeout));
            monitoringTask.reset();
            assertNull(monitoringTask.get().get());
        }
        finally
        {
            monitoringTask.reset();
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
        waitForReportToComplete();
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
        waitForReportToComplete();
    }
}
