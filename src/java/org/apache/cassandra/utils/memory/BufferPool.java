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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.NoSpamLogger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.BufferPoolMetrics;
import org.apache.cassandra.utils.concurrent.Ref;

/**
 * A pool of ByteBuffers that can be recycled.
 */
public class BufferPool
{
    // suggestion: TODO move this
    // SequentialWriter and RandomAccessReader should IMO both have their own default sizes, the latter of which we will remove soon
    // This doesn't seem to belong in this class though.
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    /** The size of a page aligned buffer, 64kbit */
    static final int CHUNK_SIZE = 64 << 10;

    @VisibleForTesting
    static long MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;

    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocateDirect(0);

    /** A global pool of chunks (page aligned buffers) */
    private static final GlobalPool globalPool = new GlobalPool();

    /** A thread local pool of chunks, where chunks come from the global pool */
    private static final ThreadLocal<LocalPool> localPool = new ThreadLocal<LocalPool>() {
        @Override
        protected LocalPool initialValue()
        {
            return new LocalPool();
        }
    };

    public static ByteBuffer get(int size)
    {
        if (size < 0)
            throw new IllegalArgumentException("Size must be positive (" + size + ")");

        if (size > 0)
            return get(size, true);

        return EMPTY_BUFFER;
    }

    public static ByteBuffer get(int size, boolean direct)
    {
        if (!direct)
            return ByteBuffer.allocate(size);
        return localPool.get().get(size);
    }

    public static void put(ByteBuffer buffer)
    {
        if (buffer.isDirect())
            localPool.get().put(buffer);
    }

    @VisibleForTesting
    static void reset()
    {
        globalPool.reset();
        localPool.set(new LocalPool());
    }

    @VisibleForTesting
    static Chunk currentChunk()
    {
        return localPool.get().currentChunk;
    }

    public static long sizeInBytes()
    {
        return globalPool.sizeInBytes();
    }

    /**
     * A queue of page aligned buffers, the buffers, which have been sliced from bigger chunks,
     * the chunks, also page aligned. Chunks are added as long as we have not exceeded the
     * memory maximum threshold, MEMORY_USAGE_THRESHOLD.
     *
     * This class is shared by multiple threads.
     */
    static final class GlobalPool
    {
        /** The size of a bigger chunk, 1mbit, must be a multiple of BUFFER_SIZE */
        static final int MACRO_CHUNK_SIZE = 1 << 20;

        static
        {
            assert Integer.bitCount(CHUNK_SIZE) == 1; // must be a power of 2
            assert Integer.bitCount(MACRO_CHUNK_SIZE) == 1; // must be a power of 2
            assert MACRO_CHUNK_SIZE % CHUNK_SIZE == 0; // must be a multiple
        }

        // the collection of large chunks we have allocated;
        // these are split up into smaller ones and placed in chunks
        private final Queue<Chunk> macroChunks = new ConcurrentLinkedQueue<>();
        // suggestion: can't we just put Chunks here? TODO
        private final Queue<ByteBuffer> chunks = new ConcurrentLinkedQueue<>();
        private final AtomicLong memoryUsage = new AtomicLong();

        public ByteBuffer get()
        {
            while (true)
            {
                ByteBuffer ret = chunks.poll();
                if (ret != null)
                    return ret;

                if (!allocateMoreChunks())
                    // give it one last attempt, in case someone else allocated before us
                    return chunks.poll();
            }
        }

        /**
         * This method might be called by multiple threads and that's fine if we add more
         * than one chunk at the same time as long as we don't exceed the MEMORY_USAGE_THRESHOLD.
         */
        private boolean allocateMoreChunks()
        {
            while (true)
            {
                long cur = memoryUsage.get();
                if (cur + MACRO_CHUNK_SIZE > MEMORY_USAGE_THRESHOLD)
                {
                    noSpamLogger.info("Maximum memory usage reached ({} bytes), cannot allocate chunk of {} bytes",
                                      MEMORY_USAGE_THRESHOLD, MACRO_CHUNK_SIZE);
                    return false;
                }
                if (memoryUsage.compareAndSet(cur, cur + MACRO_CHUNK_SIZE))
                    break;
            }

            // allocate a large chunk
            Chunk chunk = new Chunk(allocateDirectAligned(MACRO_CHUNK_SIZE));
            macroChunks.add(chunk);
            for (int i = 0 ; i < MACRO_CHUNK_SIZE ; i += CHUNK_SIZE)
            {
                chunks.add(chunk.get(CHUNK_SIZE));
            }

            return true;
        }

        public void recycle(ByteBuffer buffer)
        {
            chunks.add(buffer);
        }

        public long sizeInBytes()
        {
            return memoryUsage.get();
        }

        /** This is not thread safe and should only be used for unit testing. */
        @VisibleForTesting
        void reset()
        {
            chunks.clear();
            macroChunks.clear();
            memoryUsage.set(0);
        }
    }

    /**
     * A thread local class that slices a chunk into smaller buffers, this class is not thread
     * safe and must be called only by a single thread. Only the metrics are shared by threads.
     *
     * It grabs chunks from the global pool.
     */
    static final class LocalPool
    {
        private static final Chunk EMPTY_CHUNK = new Chunk(EMPTY_BUFFER);
        private final static BufferPoolMetrics metrics = new BufferPoolMetrics();

        public final long threadId;
        // TODO support a local queue of chunks, so we can avoid waste if we're asked to serve a range of buffer sizes
        // must be volatile so that put() from another thread correctly calls maybeRecycle()
        private volatile Chunk currentChunk;

        public LocalPool()
        {
            this.threadId = Thread.currentThread().getId();
            this.currentChunk = EMPTY_CHUNK;
        }

        private boolean grabChunk()
        {
            Chunk current = currentChunk;
            currentChunk = EMPTY_CHUNK;
            // must recycle after clearing currentChunk, so that we synchronize with a concurrent put()
            if (!current.isRecycled())
                current.maybeRecycle();

            ByteBuffer buffer = globalPool.get();
            if (buffer == null)
                return false;

            currentChunk = new Chunk(this, buffer);
            return true;
        }

        public ByteBuffer get(int size)
        {
            if (size > CHUNK_SIZE)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Requested buffer size {} is bigger than {}, allocating directly", size, CHUNK_SIZE);
                return allocate(size);
            }

            if (currentChunk.isRecycled())
            {
                if (logger.isTraceEnabled())
                    logger.trace("Current chunk was recycled by different thread", size, CHUNK_SIZE);
                if (!grabChunk())
                    return allocate(size);
            }

            ByteBuffer buffer = currentChunk.get(size);
            if (buffer != null)
                return buffer;

            if (grabChunk())
                return currentChunk.get(size);

            if (logger.isTraceEnabled())
                logger.trace("Requested buffer size {} has been allocated directly due to lack of capacity", size);

            return allocate(size);
        }

        private ByteBuffer allocate(int size)
        {
            metrics.misses.mark();
            return ByteBuffer.allocateDirect(size);
        }

        public void put(ByteBuffer buffer)
        {
            Chunk chunk = Chunk.getAllocatedFrom(buffer);
            if (chunk == null)
            {
                // suggestion: clean performs all of these checks already
                // HOWEVER: think we should consider taking a boolean indicating if a heap buffer would be ACCEPTABLE if the pool has run dry
                // this would likely be faster in most cases, since deallocation is slow. possibly we should even just ALWAYS return a heap buffer
                // if we've run out of pool space, but in this case we should probably configure unit tests to run with the pool disabled, to check we don't break anything
                FileUtils.clean(buffer);
                return;
            }

            chunk.free(buffer, this);
            if (chunk != currentChunk)
                chunk.maybeRecycle();
        }
    }

    private static ByteBuffer allocateDirectAligned(int capacity)
    {
        int align = MemoryUtil.pageSize();
        if (Integer.bitCount(align) != 1)
            throw new IllegalArgumentException("Alignment must be a power of 2");

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + align);
        long address = MemoryUtil.getAddress(buffer);
        long offset = address & (align -1); // (address % align)

        if (offset == 0)
        { // already aligned
            buffer.limit(capacity);
        }
        else
        { // shift by offset
            int pos = (int)(align - offset);
            buffer.position(pos);
            buffer.limit(pos + capacity);
        }

        return buffer.slice();
    }

    /**
     * A memory chunk, it takes a buffer and slices it
     * into smaller buffers.
     */
    final static class Chunk
    {
        public final LocalPool owner;
        private final ByteBuffer slab;
        private final long baseAddress;
        private long freeSlots = -1;

        // Incremented when the owning pool releases a buffer, single thread update
        private int normalFree;

        // Incremented when another pool releases a buffer, multiple thread update
        private volatile int atomicFree;
        private static final AtomicIntegerFieldUpdater<Chunk> atomicFreeUpdater = AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "atomicFree");

        public Chunk(ByteBuffer slab)
        {
            this(null, slab);
        }

        public Chunk(LocalPool owner, ByteBuffer slab)
        {
            assert slab.isDirect();

            this.owner = owner;
            this.slab = slab;
            this.baseAddress = MemoryUtil.getAddress(slab);

            if (slab.capacity() == 0)
                freeSlots = 0;

            this.normalFree = slab.capacity();
            this.atomicFree = 0;
        }

        public static Chunk getAllocatedFrom(ByteBuffer buffer)
        {
            Object attachment = MemoryUtil.getAttachment(buffer);
            if (attachment instanceof Chunk)
                return (Chunk) attachment;
            if (attachment instanceof Ref)
            {
                Ref<Chunk> ref = (Ref<Chunk>) attachment;
                Chunk chunk = ref.get();
                ref.release();
                return chunk;
            }
            return null;
        }

        public int capacity()
        {
            return 64 << shift();
        }

        final int unit()
        {
            return 1 << shift();
        }

        private int shift()
        {
            return (31 & (Integer.numberOfTrailingZeros(slab.capacity() / 64)));
        }

        final boolean isFree()
        {
            return free() >= capacity();
        }

        final boolean isRecycled()
        {
            return free() == capacity() + 1;
        }

        /** The total free size, includes lost free slots that could not be compacted */
        int free()
        {
            return normalFree + atomicFree;
        }

        /** Return the slab to the global pool if we can atomically increment the atomic
         * free so that our combined free count (atomic and normal) is exactly capacity + 1.
         */
        public void maybeRecycle()
        {
            if (!isFree())
                return;
            // suggestion: recycle whole Chunk, not just the slab...?
            int cur = atomicFree;
            int combined = cur + normalFree;
            if (combined > capacity() && cur != capacity() + 1)
                throw new AssertionError("Chunk has a corrupted free count (atomic=" + atomicFree + ", normal=" + normalFree + ")");
            else if (combined == capacity() && atomicFreeUpdater.compareAndSet(this, cur, cur + 1))
                globalPool.recycle(slab);
        }

        /**
         * Return the next available slice of this size. If
         * we have exceeded the capacity we return null.
         */
        public ByteBuffer get(int size)
        {
            int shift = shift();

            // how many multiples of our units is the size?
            // we add (unit - 1), so that when we divide by unit (>>> shift), we effectively round up
            int slotCount = (size - 1 + unit()) >>> shift;

            // if we require more than 64 slots, we cannot possibly accommodate the allocation
            if (slotCount > 64)
                return null;

            // convert the slotCount into the bits needed in the bitmap, but at the bottom of the register
            long slotBits = -1 >>> (64 - slotCount);

            // in order that we always allocate page aligned results, we require that any allocation is "somewhat" aligned
            // i.e. any single unit allocation can go anywhere; any 2 unit allocation must begin in one of the first 3 slots
            // of a page; a 3 unit must go in the first two slots; and any four unit allocation must be fully page-aligned

            // to achieve this, we construct a searchMask that constrains the bits we find to those we permit starting
            // a match from. as we find bits, we remove them from the mask to continue our search.
            // this has an odd property when it comes to concurrent alloc/free, as we can safely skip backwards if
            // a new slot is freed up, but we always make forward progress (i.e. never check the same bits twice),
            // so running time is bounded
            long searchMask = 0x1111111111111111L;
            searchMask *= 15L >>> ((slotCount - 1) & 3);
            // i.e. switch (slotCount & 3)
            // case 1: searchMask = 0xFFFFFFFFFFFFFFFFL
            // case 2: searchMask = 0x7777777777777777L
            // case 3: searchMask = 0x3333333333333333L
            // case 0: searchMask = 0x1111111111111111L

            // truncate the mask, removing bits that have too few slots proceeding them
            searchMask &= -1L >>> (slotCount - 1);

            // this loop is very unroll friendly, and would achieve high ILP, but not clear if the compiler will exploit this.
            // right now, not worth manually exploiting, but worth noting for future
            while (true)
            {
                long cur = freeSlots;
                // find the index of the lowest set bit that also occurs in our mask (i.e. is permitted alignment, and not yet searched)
                // we take the index, rather than finding the lowest bit, since we must obtain it anyway, and shifting is more efficient
                // than multiplication
                int index = Long.numberOfTrailingZeros(cur & searchMask);
                // if no bit was actually found, we cannot serve this request, so return null
                if (index == 64)
                    return null;
                // remove this bit from our searchMask, so we don't return here next round
                searchMask ^= 1 << index;
                // if our bits occur starting at the index, remove ourselves from the bitmask and return
                long candidate = slotBits << index;
                if ((candidate & cur) == candidate)
                {
                    freeSlots &= ~candidate;
                    return get(index << shift, slotCount << shift, size);
                }
            }
        }

        private ByteBuffer get(int offset, long roundedSize, int size)
        {
            slab.limit(offset + size);
            slab.position(offset);
            normalFree -= roundedSize;
            ByteBuffer ret = slab.slice();
            MemoryUtil.setAttachment(ret, this);
            if (Ref.DEBUG_ENABLED)
                MemoryUtil.setAttachment(ret, new Ref<>(this, null));
            return ret;
        }

        /**
         * Round the size to the next unit multiple.
         */
        int roundUp(int v)
        {
            int mask = unit() - 1;
            return (v + mask) & ~mask;
        }

        /** Release a buffer */
        public void free(ByteBuffer buffer, LocalPool pool)
        {
            // can just clear attachment, since we have a strong reference to its parent, and no point adding work for GC
            MemoryUtil.setAttachment(buffer, null);

            long address = getAddress(buffer);
            if (pool != owner)
                releaseDifferentPool(buffer);
            else
                releaseSamePool(buffer, address);
        }

        private long getAddress(ByteBuffer buffer)
        {
            long address = MemoryUtil.getAddress(buffer);

            assert (address >= baseAddress) & (address <= baseAddress + capacity());
            return address;
        }

        /** This is called by a thread local pool that has been mistakenly given
         * a byte buffer allocated by another thread local pool, we simply increment an atomic
         * counter so that we can be recycled at the end of our life and we do not
         * attempt to reuse the buffer.
         */
        public void releaseDifferentPool(ByteBuffer buffer)
        {
            if (logger.isDebugEnabled())
                logger.debug("Byte buffer {} was released from different thread: {}, original thread was {}",
                             buffer,
                             Thread.currentThread().getName(),
                             getOwningThreadName());

            int size = roundUp(buffer.capacity());
            while (true)
            {
                int cur = atomicFree;
                if (atomicFreeUpdater.compareAndSet(this, cur, cur + size))
                    break;
            }
        }

        private String getOwningThreadName()
        {
            assert owner != null;
            Thread[] threads = new Thread[Thread.activeCount()];
            Thread.enumerate(threads);
            for (Thread t : threads)
            {
                if (t.getId() == owner.threadId)
                    return t.getName();
            }
            return "Thread name not available";
        }

        /**
         *  This is called by the local thread pool owning this chunk,
         *  we try and re-use the buffer by storing it in the free slots
         *  or appending it to the end, if possible.
         *
         *  We also increment the ordinary free counter (non atomic).
         */
        private void releaseSamePool(ByteBuffer buffer, long address)
        {
            int position = (int)(address - baseAddress);
            int size = roundUp(buffer.capacity());
            int shift = shift();

            normalFree += size;

            position >>= shift;
            int slotCount = size >> shift;
            if (slotCount == 64)
            {
                assert size == capacity();
                assert position == 0;
                freeSlots = -1;
            }
            else
            {
                long slotBits = (1 << slotCount) - 1;
                freeSlots |= slotBits << position;
            }
        }

        @Override
        public String toString()
        {
            return String.format("[slab %s, slots bitmap %s, capacity %d, free %d]", slab, Long.toBinaryString(freeSlots), capacity(), free());
        }
    }
}
