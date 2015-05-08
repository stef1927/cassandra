/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.utils.memory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.io.util.RandomAccessReader;

import static org.junit.Assert.*;

public class BufferPoolTest
{
    @Before
    public void setUp()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = 8 * 1024L * 1024L;
    }

    @After
    public void cleanUp()
    {
        BufferPool.reset();
    }

    @Test
    public void testGetPut() throws InterruptedException
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;

        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertEquals(true, buffer.isDirect());

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());

        BufferPool.put(buffer);
        assertEquals(chunk, BufferPool.currentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());

        buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertEquals(chunk, BufferPool.currentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());
    }


    @Test
    public void testPageAligned()
    {
        for (int i = RandomAccessReader.DEFAULT_BUFFER_SIZE;
                 i <= BufferPool.CHUNK_SIZE;
                 i += RandomAccessReader.DEFAULT_BUFFER_SIZE)
        {
            checkPageAligned(i);
        }
    }

    private void checkPageAligned(int size)
    {
        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        assertTrue(buffer.isDirect());

        long address = MemoryUtil.getAddress(buffer);
        assertTrue((address % MemoryUtil.pageSize()) == 0);

        BufferPool.put(buffer);
    }

    @Test
    public void testDifferentSizes() throws InterruptedException
    {
        final int size1 = 1024;
        final int size2 = 2048;

        ByteBuffer buffer1 = BufferPool.get(size1);
        assertNotNull(buffer1);
        assertEquals(size1, buffer1.capacity());

        ByteBuffer buffer2 = BufferPool.get(size2);
        assertNotNull(buffer2);
        assertEquals(size2, buffer2.capacity());

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());

        BufferPool.put(buffer1);
        BufferPool.put(buffer2);

        assertEquals(chunk, BufferPool.currentChunk());
        assertEquals(BufferPool.GlobalPool.MACRO_CHUNK_SIZE, BufferPool.sizeInBytes());
    }

    @Test
    public void testMaxMemoryExceeded()
    {
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceeded_SameAsChunkSize()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = BufferPool.GlobalPool.MACRO_CHUNK_SIZE;
        requestDoubleMaxMemory();
    }

    @Test
    public void testMaxMemoryExceeded_SmallerThanChunkSize()
    {
        BufferPool.MEMORY_USAGE_THRESHOLD = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / 2;
        requestDoubleMaxMemory();
    }

    @Test
    public void testRecycle()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, 3 * BufferPool.CHUNK_SIZE, true);
    }

    private void requestDoubleMaxMemory()
    {
        requestUpToSize(RandomAccessReader.DEFAULT_BUFFER_SIZE, (int)(2 * BufferPool.MEMORY_USAGE_THRESHOLD), false);
    }

    private void requestUpToSize(int bufferSize, int totalSize, boolean giveBack)
    {
        final int numBuffers = totalSize / bufferSize;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
        {
            ByteBuffer buffer = BufferPool.get(bufferSize);
            assertNotNull(buffer);
            assertEquals(bufferSize, buffer.capacity());

            if (giveBack)
                buffers.add(buffer);
        }

        for (ByteBuffer buffer : buffers)
            BufferPool.put(buffer);

    }

    @Test
    public void testBigRequest()
    {
        final int size = BufferPool.CHUNK_SIZE + 1;

        ByteBuffer buffer = BufferPool.get(size);
        assertNotNull(buffer);
        assertEquals(size, buffer.capacity());
        BufferPool.put(buffer);
    }

    @Test
    public void testFillUpChunks()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        final int numBuffers = BufferPool.CHUNK_SIZE / size;

        List<ByteBuffer> buffers1 = new ArrayList<>(numBuffers);
        List<ByteBuffer> buffers2 = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++)
            buffers1.add(BufferPool.get(size));

        BufferPool.Chunk chunk1 = BufferPool.currentChunk();
        assertNotNull(chunk1);

        for (int i = 0; i < numBuffers; i++)
            buffers2.add(BufferPool.get(size));

        BufferPool.Chunk chunk2 = BufferPool.currentChunk();
        assertNotNull(chunk2);

        assertNotSame(chunk1, chunk2);

        for (ByteBuffer buffer : buffers1)
            BufferPool.put(buffer);

        assertEquals(chunk2, BufferPool.currentChunk());

        for (ByteBuffer buffer : buffers2)
            BufferPool.put(buffer);

        assertEquals(chunk2, BufferPool.currentChunk());

        for (int i = 0; i < numBuffers; i++)
            buffers2.add(BufferPool.get(size));

        assertEquals(chunk2, BufferPool.currentChunk());
    }

    @Test
    public void testReuseSameBuffer()
    {
        final int size = RandomAccessReader.DEFAULT_BUFFER_SIZE;
        ByteBuffer buffer = BufferPool.get(size);
        assertEquals(size, buffer.capacity());
        final long address = MemoryUtil.getAddress(buffer);
        BufferPool.put(buffer);

        buffer = BufferPool.get(size);
        assertEquals(size, buffer.capacity());
        assertEquals(address, MemoryUtil.getAddress(buffer));
        BufferPool.put(buffer);

        buffer = BufferPool.get(size);
        assertEquals(size, buffer.capacity());
        assertEquals(address, MemoryUtil.getAddress(buffer));
        BufferPool.put(buffer);
    }

    @Test
    public void testOutOfOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testInOrderFrees()
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = maxFreeSlots - 1 - i;

        doTestFrees(size, maxFreeSlots, idxs);
    }

    @Test
    public void testRandomFrees()
    {
        doTestRandomFrees(12345567878L);

        BufferPool.reset();
        doTestRandomFrees(20452249587L);

        BufferPool.reset();
        doTestRandomFrees(82457252948L);

        BufferPool.reset();
        doTestRandomFrees(98759284579L);

        BufferPool.reset();
        doTestRandomFrees(19475257244L);
    }

    private void doTestRandomFrees(long seed)
    {
        final int size = 4096;
        final int maxFreeSlots = BufferPool.CHUNK_SIZE / size;

        final int[] idxs = new int[maxFreeSlots];
        for (int i = 0; i < maxFreeSlots; i++)
            idxs[i] = maxFreeSlots - 1 - i;

        Random rnd = new Random();
        rnd.setSeed(seed);
        for (int i = idxs.length - 1; i > 0; i--)
        {
            int idx = rnd.nextInt(i+1);
            int v = idxs[idx];
            idxs[idx] = idxs[i];
            idxs[i] = v;
        }

        doTestFrees(size, maxFreeSlots, idxs);
    }

    private void doTestFrees(final int size, final int maxFreeSlots, final int[] toReleaseIdxs)
    {
        List<ByteBuffer> buffers = new ArrayList<>(maxFreeSlots);
        for (int i = 0; i < maxFreeSlots; i++)
        {
            buffers.add(BufferPool.get(size));
        }

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertFalse(chunk.isFree());

        int freeSize = BufferPool.CHUNK_SIZE - maxFreeSlots * size;
        assertEquals(freeSize, chunk.free());

        for (int i : toReleaseIdxs)
        {
            ByteBuffer buffer = buffers.get(i);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());

            BufferPool.put(buffer);
            assertEquals(chunk, BufferPool.currentChunk());

            freeSize += size;
            assertEquals(freeSize, chunk.free());
        }

        assertTrue(chunk.isFree());
    }

    @Test
    public void testDifferentSizeBuffersOnOneChunk()
    {
        int[] sizes = new int[] {
            5, 1024, 4096, 8, 16000, 78, 512, 256, 63, 55, 89, 90, 255, 32, 2048, 128
        };

        int sum = 0;
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
        {
            ByteBuffer buffer = BufferPool.get(sizes[i]);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += BufferPool.currentChunk().roundUp(buffer.capacity());
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);

        Random rnd = new Random();
        rnd.setSeed(298347529L);
        while (!buffers.isEmpty())
        {
            int index = rnd.nextInt(buffers.size());
            ByteBuffer buffer = buffers.remove(index);

            BufferPool.put(buffer);
        }

        assertEquals(chunk, BufferPool.currentChunk());
        assertTrue(chunk.isFree());
    }

    @Test
    public void testChunkExhausted()
    {
        final int size = BufferPool.CHUNK_SIZE / 64; // 1kbit
        int[] sizes = new int[128];
        Arrays.fill(sizes, size);

        int sum = 0;
        List<ByteBuffer> buffers = new ArrayList<>(sizes.length);
        for (int i = 0; i < sizes.length; i++)
        {
            ByteBuffer buffer = BufferPool.get(sizes[i]);
            assertNotNull(buffer);
            assertTrue(buffer.capacity() >= sizes[i]);
            buffers.add(buffer);

            sum += buffer.capacity();
        }

        // else the test will fail, adjust sizes as required
        assertTrue(sum <= BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);

        for (int i = 0; i < sizes.length; i++)
        {
            BufferPool.put(buffers.get(i));
        }

        assertEquals(chunk, BufferPool.currentChunk());
        assertTrue(chunk.isFree());
    }

    @Test
    public void testCompactIfOutOfCapacity()
    {
        final int size = 4096;
        final int numBuffersInChunk = BufferPool.GlobalPool.MACRO_CHUNK_SIZE / size;

        List<ByteBuffer> buffers = new ArrayList<>(numBuffersInChunk);
        Set<Long> addresses = new HashSet<>(numBuffersInChunk);

        for (int i = 0; i < numBuffersInChunk; i++)
        {
            ByteBuffer buffer = BufferPool.get(size);
            buffers.add(buffer);
            addresses.add(MemoryUtil.getAddress(buffer));
        }

        for (int i = numBuffersInChunk - 1; i >= 0; i--)
            BufferPool.put(buffers.get(i));

        for (int i = 0; i < numBuffersInChunk; i++)
        {
            ByteBuffer buffer = BufferPool.get(size);
            assertNotNull(buffer);
            assertEquals(size, buffer.capacity());
            addresses.remove(MemoryUtil.getAddress(buffer));
        }

        assertTrue(addresses.isEmpty()); // all 5 released buffers were used
    }

    @Test
    public void testHeapBuffer()
    {
        ByteBuffer buffer = BufferPool.get(1024, false);
        assertNotNull(buffer);
        assertEquals(1024, buffer.capacity());
        assertFalse(buffer.isDirect());
        assertNotNull(buffer.array());
        BufferPool.put(buffer);
    }

    @Test
    public void testRoundUp()
    {
        assertEquals(0, BufferPool.get(0).capacity());

        assertEquals(1, BufferPool.get(1).capacity());
        assertEquals(2, BufferPool.get(2).capacity());
        assertEquals(4, BufferPool.get(4).capacity());
        assertEquals(5, BufferPool.get(5).capacity());
        assertEquals(8, BufferPool.get(8).capacity());
        assertEquals(16, BufferPool.get(16).capacity());
        assertEquals(32, BufferPool.get(32).capacity());
        assertEquals(64, BufferPool.get(64).capacity());

        assertEquals(65, BufferPool.get(65).capacity());
        assertEquals(127, BufferPool.get(127).capacity());
        assertEquals(128, BufferPool.get(128).capacity());

        assertEquals(129, BufferPool.get(129).capacity());
        assertEquals(255, BufferPool.get(255).capacity());
        assertEquals(256, BufferPool.get(256).capacity());

        assertEquals(512, BufferPool.get(512).capacity());
        assertEquals(1024, BufferPool.get(1024).capacity());
        assertEquals(2048, BufferPool.get(2048).capacity());
        assertEquals(4096, BufferPool.get(4096).capacity());
        assertEquals(8192, BufferPool.get(8192).capacity());
        assertEquals(16384, BufferPool.get(16384).capacity());

        assertEquals(16385, BufferPool.get(16385).capacity());
        assertEquals(32767, BufferPool.get(32767).capacity());
        assertEquals(32768, BufferPool.get(32768).capacity());

        assertEquals(32769, BufferPool.get(32769).capacity());
        assertEquals(65535, BufferPool.get(65535).capacity());
        assertEquals(65536, BufferPool.get(65536).capacity());

        assertEquals(65537, BufferPool.get(65537).capacity());
    }

    @Test
    public void testZeroSizeRequest()
    {
        ByteBuffer buffer = BufferPool.get(0);
        assertNotNull(buffer);
        assertEquals(0, buffer.capacity());
        BufferPool.put(buffer);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeSizeRequest()
    {
        BufferPool.get(-1);
    }

    @Test
    public void testMT_SameSizeImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, true, RandomAccessReader.DEFAULT_BUFFER_SIZE);
    }

    @Test
    public void testMT_SameSizePostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, false, RandomAccessReader.DEFAULT_BUFFER_SIZE);
    }

    @Test
    public void testMT_TwoSizesOneBufferImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, true, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesOneBufferPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 1, false, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesTwoBuffersImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 2, true, 1024, 2048);
    }

    @Test
    public void testMT_TwoSizesTwoBuffersPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40, 2, false, 1024, 2048);
    }

    @Test
    public void testMT_MultipleSizesOneBufferImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             1,
                             true,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesOneBufferPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             1,
                             false,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesMultipleBuffersImmediateReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             4,
                             true,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    @Test
    public void testMT_MultipleSizesMultipleBuffersPostponedReturn() throws InterruptedException
    {
        checkMultipleThreads(40,
                             3,
                             false,
                             1024,
                             2048,
                             3072,
                             4096,
                             5120);
    }

    private void checkMultipleThreads(int threadCount, int numBuffersPerThread, final boolean returnImmediately, final int ... sizes) throws InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            final int[] threadSizes = new int[numBuffersPerThread];
            for (int j = 0; j < threadSizes.length; j++)
                threadSizes[j] = sizes[(i * numBuffersPerThread + j) % sizes.length];

            final Random rand = new Random();
            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        Thread.sleep(rand.nextInt(3));

                        List<ByteBuffer> toBeReturned = new ArrayList<ByteBuffer>(threadSizes.length);

                        for (int j = 0; j < threadSizes.length; j++)
                        {
                            ByteBuffer buffer = BufferPool.get(threadSizes[j]);
                            assertNotNull(buffer);
                            assertEquals(threadSizes[j], buffer.capacity());

                            for (int i = 0; i < 10; i++)
                                buffer.putInt(i);

                            buffer.rewind();

                            Thread.sleep(rand.nextInt(3));

                            for (int i = 0; i < 10; i++)
                                assertEquals(i, buffer.getInt());

                            if (returnImmediately)
                                BufferPool.put(buffer);
                            else
                                toBeReturned.add(buffer);

                            assertTrue(BufferPool.sizeInBytes() > 0);
                        }

                        Thread.sleep(rand.nextInt(3));

                        for (ByteBuffer buffer : toBeReturned)
                            BufferPool.put(buffer);
                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        fail(ex.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();
    }

    @Test
    public void testMultipleThreadsReleaseSameBuffer() throws InterruptedException
    {
        doMultipleThreadsReleaseBuffers(45, 4096);
    }

    @Test
    public void testMultipleThreadsReleaseDifferentBuffer() throws InterruptedException
    {
        doMultipleThreadsReleaseBuffers(45, 4096, 8192);
    }

    private void doMultipleThreadsReleaseBuffers(final int threadCount, final int ... sizes) throws InterruptedException
    {
        final ByteBuffer[] buffers = new ByteBuffer[sizes.length];
        int sum = 0;
        for (int i = 0; i < sizes.length; i++)
        {
            buffers[i] = BufferPool.get(sizes[i]);
            assertNotNull(buffers[i]);
            assertEquals(sizes[i], buffers[i].capacity());
            sum += BufferPool.currentChunk().roundUp(buffers[i].capacity());
        }

        final BufferPool.Chunk chunk = BufferPool.currentChunk();
        assertNotNull(chunk);
        assertFalse(chunk.isFree());

        // if we use multiple chunks the test will fail, adjust sizes accordingly
        assertTrue(sum < BufferPool.GlobalPool.MACRO_CHUNK_SIZE);

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        final CountDownLatch finished = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++)
        {
            final int idx = i % sizes.length;
            final ByteBuffer buffer = buffers[idx];

            executorService.submit(new Runnable()
            {
                @Override public void run()
                {
                    try
                    {
                        assertNotSame(chunk, BufferPool.currentChunk());

                        BufferPool.put(buffer);

                        if (chunk.isFree())
                            assertTrue(chunk.isRecycled());

                    }
                    catch (Exception ex)
                    {
                        ex.printStackTrace();
                        fail(ex.getMessage());
                    }
                    finally
                    {
                        finished.countDown();
                    }
                }
            });
        }

        finished.await();

        assertTrue(BufferPool.currentChunk().isFree());
        for(ByteBuffer buffer : buffers)
            BufferPool.put(buffer);
        assertTrue(BufferPool.currentChunk().isFree());

        //make sure main thread picks another chunk if its chunk was recycled by mistake
        ByteBuffer buffer = BufferPool.get(sizes[0]);
        assertNotNull(buffer);
        assertEquals(sizes[0], buffer.capacity());

        assertNotSame(chunk, BufferPool.currentChunk());
    }
}
