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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joptsimple.internal.Strings;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;

public class MonitoringTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);

    private final static MonitoringTask instance = new MonitoringTask();
    static
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(instance,
                                                                 0,
                                                                 DatabaseDescriptor.getMonitoringCheckIntervalMillis(),
                                                                 TimeUnit.MILLISECONDS);
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

        int reportInterval = DatabaseDescriptor.getMonitoringReportIntervalMillis();
        if (reportInterval < 0)
            return;

        if (now - reportTime >= reportInterval)
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
        logger.debug("Operation {} aborted", operation.name());

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
            logger.info("Operations timed out in the last {} msecs:\n{}", now - reportTime, logMsg);

        failedOperations.clear();
        reportTime = now;
        return logMsg;
    }

    String getFailedOperationsLog()
    {
        final int configMaxTimedoutOperations = DatabaseDescriptor.getMonitoringMaxTimedoutOperations();
        final long maxTimedoutOperations = configMaxTimedoutOperations < 0 ? Long.MAX_VALUE : configMaxTimedoutOperations;
        if (failedOperations.isEmpty() ||
            maxTimedoutOperations == 0)
            return Strings.EMPTY;

        final StringBuilder ret = new StringBuilder();
        failedOperations.entrySet()
                        .stream()
                        .sorted((o1, o2) -> Integer.compare(o2.getValue().size(), o1.getValue().size()))
                        .limit(maxTimedoutOperations)
                        .forEach(entry -> formatEntry(ret, entry));

        if (failedOperations.size() > maxTimedoutOperations)
            ret.append(Strings.LINE_SEPARATOR)
               .append("...");

        return ret.toString();
    }

    private static void formatEntry(StringBuilder ret, Map.Entry<String, List<Failure>> entry)
    {
        String name = entry.getKey();
        if (ret.length() > 0)
            ret.append(Strings.LINE_SEPARATOR);

        List<Failure> failures = entry.getValue();
        if (failures.size() == 1)
        {
            ret.append(name)
               .append(": ")
               .append(formatFailure(failures.get(0)));
        }
        else
        {
            ret.append(name)
               .append(String.format(" (timed out %d times): ", failures.size()))
               .append(formatFailure(Iterables.getFirst(failures, null)))
               .append(failures.size() > 2 ? " ... " : ", ")
               .append(formatFailure(Iterables.getLast(failures, null)));
        }
    }

    private static String formatFailure(Failure failure)
    {
        Monitorable operation = failure.operation;
        return String.format("received/failed %1$tT.%1$tL/%2$tT.%2$tL - timeout %3$d %4$s",
                             new Date(operation.constructionTime().timestamp),
                             new Date(failure.failedAt),
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
    }
}
