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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.System.getProperty;

/**
 * A task for monitoring in progress operations, currently only read queries, and aborting them if they time out.
 * We also log timed out operations, see CASSANDRA-7392.
 */
public class MonitoringTask
{
    private static final String LINE_SEPARATOR = getProperty("line.separator");
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);

    /**
     * Defines the interval for checking the inbound queue of failed operations.
     */
    private static final int CHECK_INTERVAL_MS = Math.max(50, Integer.valueOf(System.getProperty("monitoring_check_interval_ms", "500")));

    /**
     * Defines the interval for reporting any operations that have timed out.
     */
    private static final int REPORT_INTERVAL_MS = Math.max(0, Integer.valueOf(System.getProperty("monitoring_report_interval_ms", "5000")));

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit.
     */
    private static final int MAX_TIMEDOUT_OPERATIONS = Integer.valueOf(System.getProperty("monitoring_max_timedout_operations", "50"));

    @VisibleForTesting
    static MonitoringTask instance = make(CHECK_INTERVAL_MS, REPORT_INTERVAL_MS, MAX_TIMEDOUT_OPERATIONS);

    private final long reportIntervalMillis;
    private final int maxTimedoutOperations;
    private final BlockingQueue<FailedOperation> failedOperationsQueue;
    private final HashMap<String, FailedOperation> failedOperations;
    private final ScheduledFuture<?> checkingTask;
    private boolean failedOperationsTruncated;
    private long reportTime;

    @VisibleForTesting
    static MonitoringTask make(long checkIntervalMills, int reportIntervalMillis, int maxTimedoutOperations)
    {
        if (instance != null)
        {
            instance.cancel();
            instance = null;
        }

        return new MonitoringTask(checkIntervalMills, reportIntervalMillis, maxTimedoutOperations);
    }

    private MonitoringTask(long checkIntervalMills, int reportIntervalMillis, int maxTimedoutOperations)
    {
        this.reportIntervalMillis = reportIntervalMillis;
        this.maxTimedoutOperations = maxTimedoutOperations;
        this.failedOperationsQueue = maxTimedoutOperations > 0 ? new ArrayBlockingQueue<>(maxTimedoutOperations) : new LinkedBlockingQueue<>();
        this.failedOperations = new HashMap<>();
        this.failedOperationsTruncated = false;
        this.reportTime = ApproximateTime.currentTimeMillis();

        logger.info("Scheduling monitoring task with check interval of {} and report interval of {} ms, max timedout operations {}",
                    checkIntervalMills,
                    reportIntervalMillis,
                    maxTimedoutOperations);

        this.checkingTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> checkFailedOperations(ApproximateTime.currentTimeMillis()),
                                                                                     0,
                                                                                     checkIntervalMills,
                                                                                     TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        checkingTask.cancel(false);
    }

    public static void addFailedOperation(Monitorable operation, long now)
    {
        instance.innerAddFailedOperation(operation, now);
    }

    private void innerAddFailedOperation(Monitorable operation, long now)
    {
        if (maxTimedoutOperations == 0)
            return; // logging of failed operations disabled

        try
        {
            failedOperationsQueue.add(new FailedOperation(operation, now));
        }
        catch (IllegalStateException e)
        {
            failedOperationsTruncated = true; //queue is full
        }
    }

    private void checkFailedOperations(long now)
    {
        checkFailedOperations(now, logger.isDebugEnabled());
    }

    @VisibleForTesting
    void checkFailedOperations(long now, boolean log)
    {
        aggregateFailedOperations();

        if (now - reportTime >= reportIntervalMillis)
            logFailedOperations(now, log);
    }

    @VisibleForTesting
    void aggregateFailedOperations()
    {
        while(!failedOperationsQueue.isEmpty())
        {
            FailedOperation failedOperation = failedOperationsQueue.remove();
            FailedOperation existing = failedOperations.get(failedOperation.name());
            if (existing != null)
            {
                existing.addTimeout(failedOperation);
                continue;
            }

            if (maxTimedoutOperations > 0 && failedOperations.size() >= maxTimedoutOperations)
                failedOperationsTruncated = true;
            else
                failedOperations.put(failedOperation.name(), failedOperation);
        }
    }

    @VisibleForTesting
    List<String> getFailedOperations()
    {
        aggregateFailedOperations();

        String ret = failedOperations.isEmpty() ? "" : getFailedOperationsLog();
        updateReportingState(System.currentTimeMillis());
        return ret.isEmpty() ? Collections.emptyList() : Arrays.asList(ret.split("\n"));
    }

    private void logFailedOperations(long now, boolean log)
    {
        if (!failedOperations.isEmpty())
        {
            noSpamLogger.warn("Some operations timed out, check debug log");
            if (log)
                logger.debug("Operations that timed out in the last {} msecs:{}{}",
                             now - reportTime,
                             LINE_SEPARATOR,
                             getFailedOperationsLog());
        }

        updateReportingState(now);
    }

    private void updateReportingState(long now)
    {
        reportTime = now;
        failedOperations.clear();
        failedOperationsTruncated = false;
    }

    String getFailedOperationsLog()
    {
        final StringBuilder ret = new StringBuilder();
        failedOperations.values().forEach(o -> formatOperation(ret, o));

        if (failedOperationsTruncated)
            ret.append(LINE_SEPARATOR)
               .append("...");

        return ret.toString();
    }

    private static void formatOperation(StringBuilder ret, FailedOperation operation)
    {
        if (ret.length() > 0)
            ret.append(LINE_SEPARATOR);

        ret.append(operation.getLogMessage());
    }

    private final static class FailedOperation
    {
        public final Monitorable operation;
        public final long failedAt;
        public int numTimeouts;
        public long avgTime;
        public long maxTime;
        public long minTime;
        private String name;

        FailedOperation(Monitorable operation, long failedAt)
        {
            this.operation = operation;
            this.failedAt = failedAt;
            numTimeouts = 1;
            avgTime = failedAt - operation.constructionTime().timestamp;
            minTime = avgTime;
            maxTime = avgTime;
        }

        public String name()
        {
            if (name == null)
                name = operation.name();
            return name;
        }

        public long constructionTime()
        {
            return operation.constructionTime().timestamp;
        }

        void addTimeout(FailedOperation operation)
        {
            numTimeouts++;

            long opTime = operation.failedAt - operation.constructionTime();
            avgTime = (avgTime + opTime) / 2;

            if (opTime > maxTime)
                maxTime = opTime;
            else if (opTime < minTime)
                minTime = opTime;
        }

        public String getLogMessage()
        {
            if (numTimeouts == 1)
                return String.format("%s: total time %d msec - timeout %d %s",
                                     name(),
                                     avgTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
            else
                return String.format("%s (timed out %d times): total time avg/min/max %d/%d/%d msec - timeout %d %s",
                                     name(),
                                     numTimeouts,
                                     avgTime,
                                     minTime,
                                     maxTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
        }
    }
}
