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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

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
     * A queue of page aligned buffers, which have been sliced from bigger chunks,
     * the macro-chunks, also page aligned. Chunks are allocated as long as we have not exceeded the
     * memory maximum threshold, MEMORY_USAGE_THRESHOLD.
     *
     * This class is shared by multiple threads.
     */
    static final class GlobalPool
    {
        /** The size of a bigger chunk, 1-mbit, must be a multiple of BUFFER_SIZE */
        static final int MACRO_CHUNK_SIZE = 1 << 20;

        static
        {
            assert Integer.bitCount(CHUNK_SIZE) == 1; // must be a power of 2
            assert Integer.bitCount(MACRO_CHUNK_SIZE) == 1; // must be a power of 2
            assert MACRO_CHUNK_SIZE % CHUNK_SIZE == 0; // must be a multiple
        }

        // the collection of large chunks we have allocated;
        private final Queue<Chunk> macroChunks = new ConcurrentLinkedQueue<>();
        // the sliced chunks that we dish out on request
        private final Queue<Chunk> chunks = new ConcurrentLinkedQueue<>();
        private final AtomicLong memoryUsage = new AtomicLong();

        public Chunk get()
        {
            while (true)
            {
                Chunk ret = chunks.poll();
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
                chunks.add(new Chunk(chunk.get(CHUNK_SIZE)));
            }

            return true;
        }

        public void recycle(Chunk chunk)
        {
            chunks.add(chunk);
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
     * A thread local class that grabs a chunk from the global pool for this thread allocations.
     * Only one thread can do the allocations but multiple threads can release the allocations.
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
            current.maybeRecycle(this);

            Chunk chunk = globalPool.get();
            if (chunk == null)
                return false;

            assert chunk.owner == null;
            chunk.setOwner(this);

            currentChunk = chunk;
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
                // if we've run out of pool space, but in this case we should probably configure unit tests to run with the pool disabled,
                // to check we don't break anything
                FileUtils.clean(buffer);
                return;
            }

            chunk.free(buffer);

            if (chunk != currentChunk)
                chunk.maybeRecycle(this);
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
        private final ByteBuffer slab;
        private final long baseAddress;
        private final long shift;

        private volatile long freeSlots;
        private static final AtomicLongFieldUpdater<Chunk> atomicFreeSlotUpdater = AtomicLongFieldUpdater.newUpdater(Chunk.class, "freeSlots");

        public volatile LocalPool owner;

        public Chunk(ByteBuffer slab)
        {
            assert slab.isDirect();

            this.slab = slab;
            this.baseAddress = MemoryUtil.getAddress(slab);
            this.shift = 31 & (Integer.numberOfTrailingZeros(slab.capacity() / 64));

            this.freeSlots = slab.capacity() == 0 ? 0 : -1;
            this.owner = null;
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

        LocalPool owner()
        {
            return this.owner;
        }

        public void setOwner(LocalPool owner)
        {
            this.owner = owner;
        }

        public int capacity()
        {
            return 64 << shift;
        }

        final int unit()
        {
            return 1 << shift;
        }

        final boolean isFree()
        {
            return free() >= capacity();
        }

        /** The total free size */
        int free()
        {
            return Long.bitCount(freeSlots) * unit();
        }

        /**
         * Return the slab to the global pool if it's the correct owner asking.
         */
        public void maybeRecycle(LocalPool owner)
        {
            if (!isFree())
                return; //TODO - relax this, should be possible to release non free chunks

            if (this.owner != owner)
                return;

            setOwner(null);
            globalPool.recycle(this);
        }

        /**
         * Return the next available slice of this size. If
         * we have exceeded the capacity we return null.
         */
        public ByteBuffer get(int size)
        {
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
                    // here we are sure we will manage to CAS successfully without changing candidate because
                    // there is only one thread allocating at the moment, the concurrency is with the release
                    // operations only
                    while (true)
                    {
                        // clear the candidate bits (freeSlots &= ~candidate)
                        if (atomicFreeSlotUpdater.compareAndSet(this, cur, cur & ~candidate))
                            break;

                        cur = freeSlots;
                        // make sure no other thread has cleared the candidate bits
                        assert ((candidate & cur) == candidate);
                    }
                    return get(index << shift, size);
                }
            }
        }

        private ByteBuffer get(int offset, int size)
        {
            slab.limit(offset + size);
            slab.position(offset);

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
        public void free(ByteBuffer buffer)
        {
            // can just clear attachment, since we have a strong reference to its parent, and no point adding work for GC
            MemoryUtil.setAttachment(buffer, null);

            long address = MemoryUtil.getAddress(buffer);
            assert (address >= baseAddress) & (address <= baseAddress + capacity());

            int position = (int)(address - baseAddress);
            int size = roundUp(buffer.capacity());

            position >>= shift;
            int slotCount = size >> shift;

            if (slotCount == 64)
            {
                assert size == capacity();
                assert position == 0;

                while (true)
                {
                    long cur = freeSlots;
                    assert cur == 0; // ensure no double free
                    if (atomicFreeSlotUpdater.compareAndSet(this, cur, -1))
                        break;
                }
            }
            else
            {
                long slotBits = (1 << slotCount) - 1;

                while (true)
                {
                    long cur = freeSlots;
                    assert (cur & (slotBits << position)) == 0; // ensure no double free
                    if (atomicFreeSlotUpdater.compareAndSet(this, cur, cur | (slotBits << position)))
                        break;
                }
            }
        }

        @Override
        public String toString()
        {
            return String.format("[slab %s, slots bitmap %s, capacity %d, free %d]", slab, Long.toBinaryString(freeSlots), capacity(), free());
        }
    }
}
