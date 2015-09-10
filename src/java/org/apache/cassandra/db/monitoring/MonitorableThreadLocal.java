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

public class MonitorableThreadLocal extends ThreadLocal<AtomicReference<Monitorable>>
{
    @Override
    public AtomicReference<Monitorable> initialValue()
    {
        final AtomicReference<Monitorable> ref = new AtomicReference<>(null);
        MonitoringTask.add(ref);
        return ref;
    }

    /**
     * Change the monitorable stored in the reference stored in this thread local.
     * Only one thread will update its thread local so lazySet is enough.
     */
    public void update(Monitorable monitorable)
    {
        AtomicReference<Monitorable> ref = get();
        ref.lazySet(monitorable);
    }
}
