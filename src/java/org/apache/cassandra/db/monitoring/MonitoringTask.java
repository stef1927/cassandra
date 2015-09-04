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
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;

public class MonitoringTask implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(MonitoringTask.class);

    // These are not private for testing
    final static long CHECK_DELAY_MILLIS = 10;
    final static long REPORT_DELAY_MILLIS = 5000;

    private final static MonitoringTask instance = new MonitoringTask();
    static
    {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(instance, 0, CHECK_DELAY_MILLIS, TimeUnit.MILLISECONDS);
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

        if (now - reportTime >= REPORT_DELAY_MILLIS)
        {
            report(now);
            reportTime = now;
        }
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

    private void report(long now)
    {
        if (failedOperations.isEmpty())
            return;

        for (Map.Entry<String, List<Failure>> entry : failedOperations.entrySet())
        {
            String name = entry.getKey();
            List<Failure> failures = entry.getValue();

            StringBuilder str = new StringBuilder();
            str.append(String.format("'%s' timed out %d %s in the last %d msecs: ",
                                     name,
                                     failures.size(),
                                     failures.size() > 1 ? "times" : "time",
                                     now - reportTime));

            Iterator<Failure> it = failures.iterator();
            while(it.hasNext())
            {
                Failure failure = it.next();
                Monitorable operation = failure.operation;
                str.append(String.format("[received/failed %1$tT.%1$tL/%2$tT.%2$tL timeout %3$d %4$s]",
                                         new Date(operation.constructionTime().timestamp),
                                         new Date(failure.failedAt),
                                         operation.timeout(),
                                         operation.constructionTime().isCrossNode ? "msec/cross-node" : "msec"));

                if (it.hasNext())
                    str.append(", ");
            }

            logger.info(str.toString());
        }

        failedOperations.clear();
    }

    @VisibleForTesting
    static int numFailedOperations()
    {
        return instance.failedOperations.size();
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
