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

import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 *
 */
class AsyncPageWriter
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPageWriter.class);

    private final RateLimiter limiter;
    private final PageQueue pages;
    private final Writer writer;

    AsyncPageWriter(Connection connection, int maxPagesPerSecond, long timeoutMillis)
    {
        this.limiter = RateLimiter.create(maxPagesPerSecond > 0 ? maxPagesPerSecond : Double.MAX_VALUE);
        this.pages = new PageQueue(timeoutMillis);
        this.writer = new Writer(connection.channel(), pages);
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
            pages.offer(frame);
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

//    private void writeToChannel(Object obj)
//    {
//        Channel channel = connection.channel();
//        ChannelPromise promise = channel.newPromise();
//        promise.addListener(fut -> {
//            if (!fut.isSuccess())
//            {
//                Throwable t = fut.cause();
//                JVMStabilityInspector.inspectThrowable(t);
//                logger.error("Failed writing {} for {}", obj, options.uuid, t);
//            }
//        });
//
//        channel.writeAndFlush(obj, promise);
//    }

    /**
     * A runnable that can be submitted to the Netty event loop
     * in order to write and flush synchronously. If it cannot
     * keep up then the producer will be blocked by the pages queue.
     */
    private final static class Writer implements Runnable
    {
        private final Channel channel;
        private final PageQueue queue;
        private final AtomicBoolean completed;

        public Writer(Channel channel, PageQueue queue)
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
                boolean written = false;
                Frame page = queue.poll();
                while (page != null)
                {
                    written = true;
                    channel.write(page, channel.voidPromise());
                    page = queue.poll();
                }

                if (written)
                    channel.flush();
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error("Failed to write page to queue with error {}", t);
            }
        }
    }

    /**
     * A non-blocking bounded queue of results. This is an ad-hoc adaptation of the LMAX disruptor that
     * supports a single producer and a single consumer (Netty guarantees all channel operations are
     * performed by the channel thread). The waiting strategy is a loop with
     * a Thread.yield. See the technical paper at https://lmax-exchange.github.io/disruptor/ for more details.
     */
    private final static class PageQueue
    {
        static int LENGTH = 4; // this should be a power of two
        final long timeoutMillis;
        Frame[] pages = new Frame[LENGTH];

        volatile int producerIndex = -1; // the index where the producer is writing to
        volatile int publishedIndex = -1; // the index where the producer has written to
        volatile int consumerIndex = 0; // the index where the consumer is reading from

        PageQueue(long timeoutMillis)
        {
            this.timeoutMillis = timeoutMillis;
            assert (pages.length & (pages.length - 1)) == 0 : "Results length must be a power of 2";
        }

        // this return the modulo for a power of two
        private int index(int i)
        {
            return i & (pages.length - 1);
        }

        int size()
        {
            return publishedIndex - consumerIndex + 1;
        }

        boolean isEmpty()
        {
            return size() == 0;
        }

        Frame poll() {
            if (consumerIndex > publishedIndex)
                return null;

            if (logger.isTraceEnabled())
                logger.trace("Reading from index {}...", consumerIndex);

            Frame ret = pages[index(consumerIndex)];
            pages[index(consumerIndex)] = null;
            consumerIndex++;
            return ret;
        }

        public void offer(Frame frame)
        {
            long start = ApproximateTime.currentTimeMillis();

            producerIndex++;
            while(producerIndex - consumerIndex >= pages.length)
            { // wait for consumer to read
                Thread.yield();

                if (ApproximateTime.currentTimeMillis() - start > timeoutMillis)
                    throw new RuntimeException("Timed out waiting consumer to catch up");
            }

            if (logger.isTraceEnabled())
                logger.trace("Writing frame at index {}", producerIndex);

            pages[index(producerIndex)] = frame; // write
            publishedIndex = producerIndex; // publish
        }
    }

}
