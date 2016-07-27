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

package org.apache.cassandra.cql3.async.paging;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Writes pages to the client by scheduling events on the Netty loop that read from a queue.
 */
class AsyncPageWriter
{
    private static final int CAPACITY = 4;
    private static final Logger logger = LoggerFactory.getLogger(AsyncPageWriter.class);

    private final RateLimiter limiter;
    private final ArrayBlockingQueue<Frame> pages;
    private final Writer writer;
    private final long timeoutMillis;

    AsyncPageWriter(Connection connection, int maxPagesPerSecond, long timeoutMillis)
    {
        this.limiter = RateLimiter.create(maxPagesPerSecond > 0 ? maxPagesPerSecond : Double.MAX_VALUE);
        this.pages = new ArrayBlockingQueue<>(CAPACITY);
        this.writer = new Writer(connection.channel(), pages);
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Add a page to the queue, potentially blocking if there is no space.
     *
     * @param frame, the page
     * @param hasMorePages, whether there are more pages to follow
     */
    void sendPage(Frame frame, boolean hasMorePages)
    {
        if (writer.completed())
            throw new RuntimeException("Received unexpected page");

        try
        {
            if (!pages.offer(frame, timeoutMillis, TimeUnit.MILLISECONDS))
                throw new RuntimeException("Timed out adding page to output queue");
        }
        catch (InterruptedException ex)
        {
            frame.release();
            throw new RuntimeException("Interrupted whilst adding page to output queue");
        }
        catch (Throwable t)
        {
            frame.release();
            throw t;
        }

        writer.execute();

        if (!hasMorePages)
        {
            assert !writer.completed() : "Unexpected completed status";
            if (logger.isTraceEnabled())
                logger.trace("Completing writer");
            writer.complete();
        }
        else
        {
            limiter.acquire();
        }
    }

    /**
     * A runnable that can be submitted to the Netty event loop
     * in order to write and flush synchronously. If it cannot
     * keep up then the producer will be blocked by the pages queue.
     */
    private final static class Writer implements Runnable
    {
        private final Channel channel;
        private final ArrayBlockingQueue<Frame> queue;
        private final AtomicBoolean completed;

        public Writer(Channel channel, ArrayBlockingQueue<Frame> queue)
        {
            this.channel = channel;
            this.queue = queue;
            this.completed = new AtomicBoolean(false);
        }

        public boolean complete()
        {
            return completed.compareAndSet(false, true);
        }

        public boolean completed()
        {
            return completed.get() && queue.isEmpty();
        }

        public void execute()
        {
            channel.eventLoop().execute(this);
        }

        public void run()
        {
            try
            {
                int written = 0;
                while (written < CAPACITY)
                {
                    final Frame page = queue.poll();
                    if (page == null)
                        break;

                    written++;
                    ChannelFuture fut = channel.write(page);
                    fut.addListener(f -> {
                        if (!f.isSuccess())
                            logger.error("Failed to write {}", page, f.cause());
                    });
                }

                if (written > 0)
                    channel.flush();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Failed to write page to queue with error {}", t);
            }
        }
    }
}
