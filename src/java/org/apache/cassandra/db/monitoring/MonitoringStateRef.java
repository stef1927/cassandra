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

import java.util.concurrent.atomic.AtomicReference;

public class MonitoringStateRef extends AtomicReference<MonitoringState>
{
    public MonitoringStateRef()
    {
        set(MonitoringState.IN_PROGRESS);
    }

    public boolean completed()
    {
        return get() == MonitoringState.COMPLETED;
    }

    public boolean aborted()
    {
        return get() == MonitoringState.ABORTED;
    }

    public boolean inProgress()
    {
        return get() == MonitoringState.IN_PROGRESS;
    }

    public boolean abort()
    {
        return transitionFrom(MonitoringState.IN_PROGRESS, MonitoringState.ABORTED) || aborted();
    }

    public boolean complete()
    {
        return transitionFrom(MonitoringState.IN_PROGRESS, MonitoringState.COMPLETED) || completed();
    }

    private boolean transitionFrom(MonitoringState expected, MonitoringState updated)
    {
        MonitoringState current = get();
        if (expected != current)
            return false;

        // See discussion on CASSANDRA-7392 on why we are OK with lazySet even though it may
        // introduce some inaccuracies and return an incorrect value
        lazySet(updated);
        return true;

    }

}
