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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
public class MonitoringTask implements Runnable
{
    private static final String LINE_SEPARATOR = getProperty( "line.separator" );
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);

    /**
     * Defines the interval for reporting any operations that have timed out.
     */
    final static String REPORT_INTERVAL_PROPERTY = Config.PROPERTY_PREFIX + "monitoring_report_interval_ms";
    public static int REPORT_INTERVAL_MS = Math.max(0, Integer.valueOf(System.getProperty(REPORT_INTERVAL_PROPERTY, "5000")));

    /**
     * Defines the interval for checking if operations have timed out, it cannot be less than 50 milliseconds. If not set by default
     * this is 10% of the reporting interval.
     */
    final static String CHECK_INTERVAL_PROPERTY = Config.PROPERTY_PREFIX + "monitoring_check_interval_ms";
    public final static int CHECK_INTERVAL_MS = Math.max(50, Integer.valueOf(System.getProperty(CHECK_INTERVAL_PROPERTY, Integer.toString(REPORT_INTERVAL_MS / 10))));

    /**
     * Defines the maximum number of unique timed out queries that will be reported in the logs.
     * Use a negative number to remove any limit. Operations are sorted by number of time-outs
     * and only the top operations are reported.
     */
    final static String MAX_TIMEDOUT_OPERATIONS_PROPERTY = Config.PROPERTY_PREFIX + "monitoring_max_timedout_operations";
    public static int MAX_TIMEDOUT_OPERATIONS = Integer.valueOf(System.getProperty(MAX_TIMEDOUT_OPERATIONS_PROPERTY, "50"));

    private final static MonitoringTask instance = new MonitoringTask();
    static
    {
        logger.info("Scheduling monitoring task with check interval of {} ms and report interval of {} ms, max timedout operations {}",
                    CHECK_INTERVAL_MS,
                    REPORT_INTERVAL_MS,
                    MAX_TIMEDOUT_OPERATIONS);
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(instance, 0, CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private final CopyOnWriteArrayList<AtomicReference<Monitorable>> operations;
    private final Map<String, List<Failure>> failedOperations;
    private long reportTime;

    private MonitoringTask()
    {
        this.operations = new CopyOnWriteArrayList<>();
        this.failedOperations = new HashMap<>();
        this.reportTime = System.currentTimeMillis();
    }

    public static void add(AtomicReference<Monitorable> ref)
    {
        instance.operations.add(ref);
    }

    public void run()
    {
        final long now = System.currentTimeMillis();
        operations.forEach(o -> checkOperation(now, o));

        if (now - reportTime >= REPORT_INTERVAL_MS)
            logFailedOperations(now);
    }

    private void checkOperation(long now, AtomicReference<Monitorable> ref)
    {
        Monitorable operation = ref.get();
        if (operation == null || !operation.state().inProgress())
            return;

        long elapsed = now - operation.constructionTime().timestamp;
        if (elapsed >= operation.timeout() && operation.state().abort())
            addFailedOperation(now, operation);
    }

    private void addFailedOperation(long now, Monitorable operation)
    {
        List<Failure> failures = failedOperations.get(operation.name());
        if (failures == null)
        {
            failures = new ArrayList<>();
            failedOperations.put(operation.name(), failures);
        }

        failures.add(new Failure(operation, now));
    }

    @VisibleForTesting
    static List<String> logFailedOperations()
    {
        String ret = instance.logFailedOperations(System.currentTimeMillis());
        if (ret.isEmpty())
            return Collections.emptyList();

        return Arrays.asList(ret.split("\n"));
    }

    private String logFailedOperations(long now)
    {
        String logMsg = getFailedOperationsLog();
        if (!logMsg.isEmpty())
        {
            noSpamLogger.warn("Some operations timed out, check debug log");
            logger.debug("Operations that timed out in the last {} msecs:\n{}", now - reportTime, logMsg);
        }
        failedOperations.clear();
        reportTime = now;
        return logMsg;
    }

    String getFailedOperationsLog()
    {
        if (failedOperations.isEmpty() || MAX_TIMEDOUT_OPERATIONS == 0)
            return "";

        final long limit = MAX_TIMEDOUT_OPERATIONS < 0 ? Long.MAX_VALUE : MAX_TIMEDOUT_OPERATIONS;
        final StringBuilder ret = new StringBuilder();
        failedOperations.entrySet()
                        .stream()
                        .sorted((o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()))
                        .limit(limit)
                        .forEach(entry -> formatEntry(ret, entry));

        if (failedOperations.size() > limit)
            ret.append(LINE_SEPARATOR)
               .append("...");

        return ret.toString();
    }

    private static void formatEntry(StringBuilder ret, Map.Entry<String, List<Failure>> entry)
    {
        String name = entry.getKey();
        if (ret.length() > 0)
            ret.append(LINE_SEPARATOR);

        List<Failure> failures = entry.getValue();
        if (failures.size() == 1)
            ret.append(name)
               .append(": ")
               .append(formatFailure(failures.get(0)));
        else
            ret.append(name)
               .append(String.format(" (timed out %d times): ", failures.size()))
               .append(formatFailures(failures));
    }

    private static String formatFailure(Failure failure)
    {
        Monitorable operation = failure.operation;
        return String.format("total time %d msec - timeout %d %s",
                             failure.totalTime(),
                             operation.timeout(),
                             operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
    }

    private static String formatFailures(List<Failure> failures)
    {
        Monitorable operation = failures.get(0).operation;
        long avg = failures.stream().map(Failure::totalTime).reduce(0L, Long::sum) / failures.size();
        long min = failures.stream().map(Failure::totalTime).reduce(Long.MAX_VALUE, Long::min);
        long max = failures.stream().map(Failure::totalTime).reduce(Long.MIN_VALUE, Long::max);

        return String.format("total time avg/min/max %d/%d/%d msec - timeout %d %s",
                             avg,
                             min,
                             max,
                             operation.timeout(),
                             operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec");
    }

    private final static class Failure
    {
        public final Monitorable operation;
        public final long failedAt;

        Failure(Monitorable operation, long failedAt)
        {
            this.operation = operation;
            this.failedAt = failedAt;
        }

        long totalTime()
        {
            return failedAt - operation.constructionTime().timestamp;
        }
    }
}
