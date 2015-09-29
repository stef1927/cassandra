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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.Config;
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
     * Defines the interval for reporting any operations that have timed out.
     */
    private static final int REPORT_INTERVAL_MS = Math.max(0, Integer.valueOf(System.getProperty(Config.PROPERTY_PREFIX + "monitoring_report_interval_ms", "5000")));

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit.
     */
    private static final int MAX_OPERATIONS = Integer.valueOf(System.getProperty(Config.PROPERTY_PREFIX + "monitoring_max_operations", "50"));

    @VisibleForTesting
    static MonitoringTask instance = make(REPORT_INTERVAL_MS, MAX_OPERATIONS);

    private final int maxOperations;
    private final BlockingQueue<FailedOperation> operationsQueue;
    private final ScheduledFuture<?> reportingTask;
    private int numDroppedOperations;
    private long lastTime;

    @VisibleForTesting
    static MonitoringTask make(int reportIntervalMillis, int maxTimedoutOperations)
    {
        if (instance != null)
        {
            instance.cancel();
            instance = null;
        }

        return new MonitoringTask(reportIntervalMillis, maxTimedoutOperations);
    }

    private MonitoringTask(int reportIntervalMillis, int maxOperations)
    {
        this.maxOperations = maxOperations;
        this.operationsQueue = maxOperations > 0 ? new ArrayBlockingQueue<>(maxOperations) : new LinkedBlockingQueue<>();
        this.numDroppedOperations = 0;
        this.lastTime = ApproximateTime.currentTimeMillis();

        logger.info("Scheduling monitoring task with report interval of {} ms, max operations {}", reportIntervalMillis, maxOperations);
        this.reportingTask = ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> logFailedOperations(ApproximateTime.currentTimeMillis()),
                                                                                     reportIntervalMillis,
                                                                                     reportIntervalMillis,
                                                                                     TimeUnit.MILLISECONDS);
    }

    public void cancel()
    {
        reportingTask.cancel(false);
    }

    public static void addFailedOperation(Monitorable operation, long now)
    {
        instance.innerAddFailedOperation(operation, now);
    }

    private void innerAddFailedOperation(Monitorable operation, long now)
    {
        if (maxOperations == 0)
            return; // logging of failed operations disabled

        if (!operationsQueue.offer(new FailedOperation(operation, now)))
            numDroppedOperations++; //queue is full
    }

    @VisibleForTesting
    Map<String, FailedOperation> aggregateFailedOperations()
    {
        Map<String, FailedOperation> ret = new HashMap<>();

        FailedOperation failedOperation = operationsQueue.poll();
        while(failedOperation != null)
        {
            FailedOperation existing = ret.get(failedOperation.name());
            if (existing != null)
                existing.addTimeout(failedOperation);
            else
                ret.put(failedOperation.name(), failedOperation);

            failedOperation = operationsQueue.poll();
        }

        return ret;
    }

    @VisibleForTesting
    List<String> getFailedOperations()
    {
        String ret = getFailedOperationsLog(aggregateFailedOperations());
        updateReportingState(System.currentTimeMillis());
        return ret.isEmpty() ? Collections.emptyList() : Arrays.asList(ret.split("\n"));
    }

    private void logFailedOperations(long now)
    {
        logFailedOperations(now, logger.isDebugEnabled());
    }

    @VisibleForTesting
    void logFailedOperations(long now, boolean log)
    {
        Map<String, FailedOperation> failedOperations = aggregateFailedOperations();
        if (!failedOperations.isEmpty())
        {
            long elapsed = now - lastTime;
            noSpamLogger.warn("{} operations timed out in the last {} msecs, check debug log", failedOperations.size(), elapsed);

            if (log)
                logger.info("{} operations timed out in the last {} msecs:{}{}",
                            failedOperations.size(),
                            elapsed,
                            LINE_SEPARATOR,
                            getFailedOperationsLog(failedOperations));
        }

        updateReportingState(now);
    }

    private void updateReportingState(long now)
    {
        lastTime = now;
        numDroppedOperations = 0;
    }

    String getFailedOperationsLog(Map<String, FailedOperation> failedOperations)
    {
        if (failedOperations.isEmpty())
            return "";

        final StringBuilder ret = new StringBuilder();
        failedOperations.values().forEach(o -> formatOperation(ret, o));

        if (numDroppedOperations > 0)
            ret.append(LINE_SEPARATOR)
               .append("... (")
               .append(numDroppedOperations)
               .append(" were dropped)");

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
        public long totalTime;
        public long maxTime;
        public long minTime;
        private String name;

        FailedOperation(Monitorable operation, long failedAt)
        {
            this.operation = operation;
            this.failedAt = failedAt;
            numTimeouts = 1;
            totalTime = failedAt - operation.constructionTime().timestamp;
            minTime = totalTime;
            maxTime = totalTime;
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
            totalTime += opTime;

            maxTime = Math.max(maxTime, opTime);
            minTime = Math.min(minTime, opTime);
        }

        public String getLogMessage()
        {
            if (numTimeouts == 1)
                return String.format("%s: total time %d msec - timeout %d %s",
                                     name(),
                                     totalTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
            else
                return String.format("%s (timed out %d times): total time avg/min/max %d/%d/%d msec - timeout %d %s",
                                     name(),
                                     numTimeouts,
                                     totalTime / numTimeouts,
                                     minTime,
                                     maxTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
        }
    }
}
