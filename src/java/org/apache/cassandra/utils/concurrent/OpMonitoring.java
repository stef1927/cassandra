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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;

/**
 * A class for monitoring any operation via an OpState. When the timeout has elapsed, the
 * operation is aborted.
 */
public class OpMonitoring implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(OpMonitoring.class);

    private final long constructionTime;
    private final long timeout;
    private final OpState state;

    public static ScheduledFuture<?> schedule(long constructionTime,
                                              long timeout,
                                              OpState state)
    {
        return (new OpMonitoring(constructionTime, timeout, state)).schedule();
    }

    private OpMonitoring(long constructionTime,
                         long timeout,
                         OpState state)
    {
        this.constructionTime = constructionTime;
        this.timeout = timeout;
        this.state = state;
    }

    public void run()
    {
        if (state.abort())
        {
            logger.info("<{}> timed out (> {} milliseconds, from {} to {})",
                        state.name(),
                        timeout,
                        constructionTime,
                        System.currentTimeMillis());
        }
    }

    private ScheduledFuture<?> schedule()
    {
        long elapsed = System.currentTimeMillis() - constructionTime;
        long remaining = timeout > elapsed ? timeout - elapsed : 0;
        return ScheduledExecutors.scheduledTasks.schedule(this, remaining, TimeUnit.MILLISECONDS);
    }
}
