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

package org.apache.cassandra.batchlog;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

public class BatchCallback implements IAsyncCallbackWithFailure
{
    private static final Logger logger = LoggerFactory.getLogger(BatchCallback.class);
    public enum Outcome { NONE, SUCCESS, FAILURE }

    private final long start = System.nanoTime();
    private final SimpleCondition condition = new SimpleCondition();
    private volatile Outcome outcome = Outcome.NONE;

    public void onFailure(InetAddress from)
    {
        logger.trace("Got failure from {}", from);

        outcome = Outcome.FAILURE;
        condition.signalAll();
    }

    public void response(MessageIn msg)
    {
        outcome = Outcome.SUCCESS;
        condition.signalAll();
    }

    public void await() throws WriteTimeoutException, WriteFailureException
    {
        long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getWriteRpcTimeout()) - (System.nanoTime() - start);

        try
        {
            if (!condition.await(timeout, TimeUnit.NANOSECONDS))
            {
                logger.trace("Timed out waiting for response");
                throw new WriteTimeoutException(WriteType.BATCH_LOG, ConsistencyLevel.ONE, 0, 1);
            }

            if (outcome != Outcome.SUCCESS)
            {
                logger.trace("Failed to get response: {}", outcome);
                throw new WriteFailureException(ConsistencyLevel.ONE, 0, 1, 1, WriteType.BATCH_LOG);
            }
        }
        catch (InterruptedException ex)
        {
            throw new AssertionError(ex);
        }
    }

    public boolean isLatencyForSnitch()
    {
        return false;
    }
}
