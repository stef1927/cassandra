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
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.*;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class LongBufferPoolTest
{
    private static final Logger logger = LoggerFactory.getLogger(LongBufferPoolTest.class);

    @Test
    public void testAllocateWithPool() throws InterruptedException, ExecutionException
    {
        BufferPool.DISABLED = false;
        testAllocate(45, TimeUnit.MINUTES.toNanos(2L));
    }

    @Test
    public void testAllocateNoPool() throws InterruptedException, ExecutionException
    {
        BufferPool.DISABLED = true;
        testAllocate(45, TimeUnit.MINUTES.toNanos(2L));
    }

    public void testAllocate(int threadCount, long duration) throws InterruptedException, ExecutionException
    {
        final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        System.out.println(String.format("%s - testing %d threads for %dm",
                                         dateFormat.format(new Date()),
                                         threadCount,
                                         TimeUnit.NANOSECONDS.toMinutes(duration)));

        final long until = System.nanoTime() + duration;
        final CountDownLatch latch = new CountDownLatch(threadCount);
        final Queue<ByteBuffer> buffers = new ConcurrentLinkedDeque<>();

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> ret = new ArrayList<>(threadCount);

        for (int t = 0; t < threadCount; t++)
        {
            ret.add(executorService.submit(new Callable()
            {
                private final int minBufferSize = 12;
                private final ThreadLocalRandom rand = ThreadLocalRandom.current();
                private int size = 0;

                private void writeBuffer(ByteBuffer buffer)
                {
                    int numVals = (buffer.capacity() - 4) / 8;
                    buffer.putInt(numVals);

                    Long val = rand.nextLong(10000000L);
                    for (int i = 0; i < numVals; i++)
                        buffer.putLong(val);
                }

                private void readBuffer(ByteBuffer buffer)
                {
                    buffer.position(0);
                    int numVals = buffer.getInt();

                    long val = 0;
                    for (int i = 0; i < numVals; i++)
                    {
                        if (i ==0)
                            val = buffer.getLong();
                        else
                            assertEquals(val, buffer.getLong());
                    }
                }

                public Boolean call() throws Exception
                {
                    try
                    {
                        while (System.nanoTime() < until)
                        {
                            size = Math.max(minBufferSize, rand.nextInt(BufferPool.CHUNK_SIZE + 1));

                            ByteBuffer buffer = BufferPool.get(size);
                            assertNotNull(buffer);
                            assertEquals(size, buffer.capacity());
                            assertEquals(0, buffer.position());
                            writeBuffer(buffer);

                            Thread.sleep(rand.nextInt(1000)); // sleep up to 1 second
                            readBuffer(buffer);

                            if (rand.nextBoolean())
                            { // toss the coin, either release this buffer now or
                                BufferPool.put(buffer);
                            }
                            else
                            { // stash it for later release possibly by a different thread
                                buffers.offer(buffer);

                                if (buffers.size() > 5)
                                { // if enough buffers have been stashed, just release the first one
                                    ByteBuffer anotherBuffer = buffers.poll();
                                    readBuffer(anotherBuffer);
                                    BufferPool.put(anotherBuffer);
                                }
                            }
                        }
                        return true;
                    }
                    catch (Exception ex)
                    {
                        logger.error("Got exception {}, latest size {}, current chunk {}",
                                     ex.getMessage(),
                                     size,
                                     BufferPool.currentChunk());
                        ex.printStackTrace();
                        return false;
                    }
                    finally
                    {
                        latch.countDown();
                    }
                }
            }));
        }

        latch.await();

        for (ByteBuffer buffer : buffers)
            BufferPool.put(buffer);

        assertEquals(0, executorService.shutdownNow().size());

        for (Future<Boolean> r : ret)
            assertTrue(r.get());

        System.out.println(String.format("%s - finished.",
                                         dateFormat.format(new Date())));
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException
    {
        new LongBufferPoolTest().testAllocate(45, TimeUnit.HOURS.toNanos(2L));
    }


}