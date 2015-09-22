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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    final static String REPORT_INTERVAL_PROPERTY = Config.PROPERTY_PREFIX + "monitoring_report_interval_ms";
    public static int REPORT_INTERVAL_MS = Math.max(0, Integer.valueOf(System.getProperty(REPORT_INTERVAL_PROPERTY, "5000")));

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit.
     */
    final static String MAX_TIMEDOUT_OPERATIONS_PROPERTY = Config.PROPERTY_PREFIX + "monitoring_max_timedout_operations";
    public static int MAX_TIMEDOUT_OPERATIONS = Integer.valueOf(System.getProperty(MAX_TIMEDOUT_OPERATIONS_PROPERTY, "50"));

    private final static MonitoringTask instance = new MonitoringTask();
    static
    {
        logger.info("Scheduling monitoring task with report interval of {} ms, max timedout operations {}",
                    REPORT_INTERVAL_MS,
                    MAX_TIMEDOUT_OPERATIONS);
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> logFailedOperations(ApproximateTime.currentTimeMillis()),
                                                                 0,
                                                                 REPORT_INTERVAL_MS,
                                                                 TimeUnit.MILLISECONDS);
    }

    private final ConcurrentMap<String, FailedOperation> failedOperations;
    private boolean failedOperationsTruncated;
    private long reportTime;

    private MonitoringTask()
    {
        this.failedOperations = new ConcurrentHashMap<>();
        this.failedOperationsTruncated = false;
        this.reportTime = System.currentTimeMillis();
    }

    public static void addFailedOperation(Monitorable operation, long now)
    {
        instance.doAddFailedOperation(operation, now);
    }

    private void doAddFailedOperation(Monitorable operation, long now)
    {
        FailedOperation failedOperation = failedOperations.get(operation.name());
        if (failedOperation != null)
        {
            failedOperation.addTimeout(operation, now);
            return;
        }

        if (MAX_TIMEDOUT_OPERATIONS >= 0 && failedOperations.size() >= MAX_TIMEDOUT_OPERATIONS)
            failedOperationsTruncated = true;
        else
            failedOperations.put(operation.name(), new FailedOperation(operation, now));
    }

    @VisibleForTesting
    static List<String> getFailedOperations()
    {
        return instance.innerGetFailedOperations();
    }

    private List<String> innerGetFailedOperations()
    {
        String ret = failedOperations.isEmpty() ? "" : getFailedOperationsLog();
        updateReportingState(System.currentTimeMillis());
        return ret.isEmpty() ? Collections.emptyList() : Arrays.asList(ret.split("\n"));
    }

    @VisibleForTesting
    static void logFailedOperations(long now)
    {
        instance.innerLogFailedOperations(now);
    }

    private void innerLogFailedOperations(long now)
    {
        if (!failedOperations.isEmpty())
        {
            noSpamLogger.warn("Some operations timed out, check debug log");
            if (logger.isDebugEnabled())
                logger.debug("Operations that timed out in the last {} msecs:\n{}", now - reportTime, getFailedOperationsLog());
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
        public int numTimeouts;
        public long avgTime;
        public long maxTime;
        public long minTime;

        FailedOperation(Monitorable operation, long failedAt)
        {
            this.operation = operation;
            numTimeouts = 1;
            avgTime = failedAt - operation.constructionTime().timestamp;
            minTime = avgTime;
            maxTime = avgTime;
        }

        void addTimeout(Monitorable operation, long failedAt)
        {
            assert operation.name().equals(this.operation.name()) : "Expected identical operation name";
            numTimeouts++;

            long opTime = failedAt - operation.constructionTime().timestamp;
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
                                     operation.name(),
                                     avgTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
            else
                return String.format("%s (timed out %d times): total time avg/min/max %d/%d/%d msec - timeout %d %s",
                                     operation.name(),
                                     numTimeouts,
                                     avgTime,
                                     minTime,
                                     maxTime,
                                     operation.timeout(),
                                     operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
        }
    }
}
