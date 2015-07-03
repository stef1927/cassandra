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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/** A utility class to complete or abort an operation in a non-blocking thread safe way:
 *  IN PROGRESS -> ABORTED   (abort)
 *  IN PROGRESS -> COMPLETED (complete)
 */
public class OpState
{
    private enum State
    {
        IN_PROGRESS,
        ABORTED,
        COMPLETED
    }

    private final String name;
    private volatile State state = State.IN_PROGRESS;
    private AtomicReferenceFieldUpdater stateUpdater = AtomicReferenceFieldUpdater.newUpdater(OpState.class, State.class, "state");

    public OpState()
    {
        this("");
    }

    public OpState(String name)
    {
        this.name = name;
    }

    public boolean completed()
    {
        return state == State.COMPLETED;
    }

    public boolean aborted()
    {
        return state == State.ABORTED;
    }

    public String name()
    {
        return name;
    }

    public boolean abort()
    {
        if (transitionFrom(OpState.State.IN_PROGRESS, OpState.State.ABORTED))
            return true;

        return state == OpState.State.ABORTED;
    }

    public boolean complete()
    {
        if (transitionFrom(OpState.State.IN_PROGRESS, OpState.State.COMPLETED))
            return true;

        return state == OpState.State.COMPLETED;
    }

    private boolean transitionFrom(State expected, State updated)
    {
        while(true)
        {
            State current = state;
            if (expected != current)
                return false;

            if (stateUpdater.compareAndSet(this, current, updated))
                return true;
        }
    }

}
