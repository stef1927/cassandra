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

import static org.junit.Assert.*;

public class LongBufferPoolTest
{
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
                private final int minBufferSize = 16;
                private final long datum = 123456L;

                private void writeBuffer(ByteBuffer buffer)
                {
                    buffer.putLong(datum);
                    buffer.putLong(Thread.currentThread().getId());
                }

                private void readBuffer(ByteBuffer buffer, boolean sameThread)
                {
                    buffer.position(0);
                    assertEquals(datum, buffer.getLong());

                    long id = buffer.getLong();
                    if (sameThread)
                        assertEquals(Thread.currentThread().getId(), id);
                }

                public Boolean call() throws Exception
                {
                    try
                    {
                        ThreadLocalRandom rand = ThreadLocalRandom.current();
                        while (System.nanoTime() < until)
                        {
                            int size = Math.max(minBufferSize, rand.nextInt(BufferPool.CHUNK_SIZE + 1));

                            ByteBuffer buffer = BufferPool.get(size);
                            assertNotNull(buffer);
                            assertEquals(size, buffer.capacity());
                            assertEquals(0, buffer.position());
                            writeBuffer(buffer);

                            Thread.sleep(rand.nextInt(1000)); // sleep up to 1 second
                            readBuffer(buffer, true);

                            if (rand.nextBoolean())
                            { //toss the coin, either release this buffer now or
                                BufferPool.put(buffer);
                            }
                            else
                            { //stash it for later release, any thread can release it
                                buffers.offer(buffer);
                                if (buffers.size() > 2)
                                {
                                    ByteBuffer anotherBuffer = buffers.poll();
                                    readBuffer(anotherBuffer, false);
                                    BufferPool.put(anotherBuffer);
                                }
                            }
                        }
                        return true;
                    }
                    catch (Exception ex)
                    {
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