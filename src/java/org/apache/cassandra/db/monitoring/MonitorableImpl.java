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

public abstract class MonitorableImpl implements Monitorable
{
    private final MonitoringStateRef state;
    private ConstructionTime constructionTime;
    private long timeout;

    protected MonitorableImpl()
    {
        this.state = new MonitoringStateRef();
    }

    /**
     * This setter is ugly but the construction chain to ReadCommand
     * is too complex, it would require passing new parameters to all serializers
     * or specializing the serializers to accept these message properties.
     */
    public void setMonitoringTime(ConstructionTime constructionTime, long timeout)
    {
        this.constructionTime = constructionTime;
        this.timeout = timeout;
    }

    public MonitoringStateRef state()
    {
        return state;
    }

    public ConstructionTime constructionTime()
    {
        return constructionTime;
    }

    public long timeout()
    {
        return timeout;
    }
}
