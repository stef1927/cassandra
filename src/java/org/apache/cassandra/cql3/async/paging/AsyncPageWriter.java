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

import java.util.Arrays;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.transport.Frame;
import org.apache.cassandra.transport.Server;

/**
 * A netty chunked input composed of a queue of pages. {@link AsyncPagingService} puts pages into this queue
 * whilst a Netty ChunkWriteHandler called {@link Server#CHUNKED_WRITER} reads pages from this queue and writes
 * them to the client, when it is ready to receive them, that is when the channel is writable.
 */
class AsyncPageWriter implements ChunkedInput<Frame>
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncPageWriter.class);

    private final Channel channel;
    private final ChunkedWriteHandler handler;
    private final RateLimiter limiter;
    private final PageQueue pages;
    private volatile boolean completed;
    private volatile boolean closed;
    private int[] queueSizeDist = new int[PageQueue.LENGTH + 1];

    AsyncPageWriter(Connection connection, int maxPagesPerSecond, long timeoutMillis)
    {
        this.channel = connection.channel();
        this.handler = (ChunkedWriteHandler)channel.pipeline().get(Server.CHUNKED_WRITER);
        this.limiter = RateLimiter.create(maxPagesPerSecond > 0 ? maxPagesPerSecond : Double.MAX_VALUE);
        this.pages = new PageQueue(handler, timeoutMillis);
    }

    /**
     * Adds a page to the queue so that it can be sent later on when the channel is available.
     * This method will block for up to timeoutMillis if the queue if full.
     */
    void sendPage(Frame frame, boolean hasMorePages)
    {
        if (closed)
            throw new RuntimeException("Chunked input was closed");

        try
        {
            pages.offer(frame);
        }
        catch (Throwable t)
        {
            frame.release();
            throw t;
        }

        if (channel.isWritable() && pages.size() > 0)
            handler.resumeTransfer();

        if (!hasMorePages)
        {
            assert !completed : "Unexpected completed status";
            completed = true;
        }
        else
        {
            limiter.acquire();
        }
    }

    public boolean isEndOfInput() throws Exception
    {
        return completed && pages.isEmpty();
    }

    public void close() throws Exception
    {
        if (!closed)
        {
            closed = true;
            if (logger.isTraceEnabled())
                logger.trace("Closing chunked input, pending pages: {}, queue size dist {}",
                              pages.size(), Arrays.toString(queueSizeDist));
        }
    }

    /**
     * Removes a page from the queue and returns it to the caller, the chunked writer, which will write
     * it to the client. We can return null to indicate there is nothing to read at the moment, but if
     * we do this then we must call {@link this#handler#resumeTransfer()} later on when we do have something to read.
     * If we cannot acquire a permit from the reader, we try again after a small and random amount of time.
     *
     * @return a page to write to the client, null if no page is available.
     */
    public Frame readChunk(ChannelHandlerContext channelHandlerContext) throws Exception
    {
        if (logger.isTraceEnabled())
            queueSizeDist[pages.size()]++;

        return pages.poll();
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

        final ChunkedWriteHandler handler;

        volatile int producerIndex = -1; // the index where the producer is writing to
        volatile int publishedIndex = -1; // the index where the producer has written to
        volatile int consumerIndex = 0; // the index where the consumer is reading from

        PageQueue(ChunkedWriteHandler handler, long timeoutMillis)
        {
            this.timeoutMillis = timeoutMillis;
            this.handler = handler;
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
            long lastFlushed = start;

            producerIndex++;
            while(producerIndex - consumerIndex >= pages.length)
            { // wait for consumer to read
                Thread.yield();

                long now = ApproximateTime.currentTimeMillis();
                if (now - lastFlushed > ApproximateTime.precision()) {
                    handler.resumeTransfer();
                    lastFlushed = now;
                }

                if (now - start > timeoutMillis)
                    throw new RuntimeException("Timed out waiting consumer to catch up");
            }

            if (logger.isTraceEnabled())
                logger.trace("Writing frame at index {}", producerIndex);

            pages[index(producerIndex)] = frame; // write
            publishedIndex = producerIndex; // publish
        }
    }

}
