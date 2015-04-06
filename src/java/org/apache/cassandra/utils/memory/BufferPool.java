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
import sun.nio.ch.DirectBuffer;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.BufferPoolMetrics;

/**
 * A pool of ByteBuffers that can be recycled.
 */
public class BufferPool
{
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    @VisibleForTesting
    static long MEMORY_USAGE_THRESHOLD = DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L;

    private static final Logger logger = LoggerFactory.getLogger(BufferPool.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 15L, TimeUnit.MINUTES);
    private static final ByteBuffer EMPTY_BUFFER = Allocator.direct(0).allocate();

    /** A global pool of chunks (page aligned buffers) */
    private static final GlobalPool globalPool = new GlobalPool();

    /** A thread local pool of chunks, where chunks come from the global pool */
    private static final ThreadLocal<LocalPool> localPool = new ThreadLocal<LocalPool>() {
        @Override
        protected LocalPool initialValue()
        {
            return new LocalPool(globalPool);
        }
    };

    public static ByteBuffer get(int size)
    {
       return get(size, true);
    }

    public static ByteBuffer get(int size, boolean direct)
    {
        return localPool.get().get(size, direct);
    }

    public static void put(ByteBuffer buffer)
    {
        localPool.get().put(buffer);
    }

    @VisibleForTesting
    static void reset()
    {
        globalPool.reset();
        localPool.set(new LocalPool(globalPool));
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
        /** The size of a page aligned buffer, 64kb, if you change this you must change the Slot encoding */
        static final int BUFFER_SIZE = 65536;

        /** The size of a bigger chunk, from which the page aligned buffers are sliced, must be a multiple of BUFFER_SIZE */
        static final int CHUNK_SIZE = 1048576;

        static {
            assert Integer.bitCount(BUFFER_SIZE) == 1; // must be a power of 2
            assert Integer.bitCount(CHUNK_SIZE) == 1; // must be a power of 2
            assert CHUNK_SIZE % BUFFER_SIZE == 0; // must be a multiple
        }

        private final Allocator allocator = Allocator.aligned(CHUNK_SIZE);
        private final Queue<Chunk> chunks = new ConcurrentLinkedQueue<>();
        private final Queue<ByteBuffer> buffers = new ConcurrentLinkedQueue<>();
        private final AtomicLong memoryUsage = new AtomicLong();

        public ByteBuffer get()
        {
            while (true)
            {
                ByteBuffer ret = buffers.poll();
                if (ret != null)
                    return ret;

                if (!allocateChunk())
                    // give it one last attempt, in case someone else allocated before us
                    return buffers.poll();
            }
        }

        /**
         * This method might be called by multiple threads and that's fine if we add more
         * than one chunk at the same time as long as we don't exceed the MEMORY_USAGE_THRESHOLD.
         */
        private boolean allocateChunk()
        {
            // Yes the = was definitely a mistake, thanks for spotting it.
            while (true)
            {
                long cur = memoryUsage.get();
                if (cur + CHUNK_SIZE > MEMORY_USAGE_THRESHOLD)
                {
                    noSpamLogger.info("Maximum memory usage reached ({} bytes), cannot allocate chunk of {} bytes",
                                      MEMORY_USAGE_THRESHOLD, CHUNK_SIZE);
                    return false;
                }
                if (memoryUsage.compareAndSet(cur, cur + CHUNK_SIZE))
                    break;
            }

            Chunk chunk = new Chunk(allocator.allocate());
            chunks.add(chunk);
            while (chunk.hasCapacity(BUFFER_SIZE))
            {
                buffers.add(chunk.get(BUFFER_SIZE));
            }

            return true;
        }

        public void recycle(ByteBuffer buffer)
        {
            buffers.add(buffer);
        }

        public long sizeInBytes()
        {
            return memoryUsage.get();
        }

        /** This is not thread safe and should only be used for unit testing. */
        @VisibleForTesting
        void reset()
        {
            buffers.clear();

            while (!chunks.isEmpty())
            {
                Chunk chunk = chunks.poll();
                Allocator.deallocate(chunk.slab);
            }

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

        private final GlobalPool pool;
        public final long threadId;
        private Chunk currentChunk;

        public LocalPool(GlobalPool pool)
        {
            this.pool = pool;
            this.threadId = Thread.currentThread().getId();
            this.currentChunk = EMPTY_CHUNK;
        }

        private void grabChunk()
        {
            if (currentChunk != EMPTY_CHUNK)
            {
                recycle(currentChunk);
            }

            ByteBuffer buffer = pool.get();
            if (buffer == null)
            {
                currentChunk = EMPTY_CHUNK;
            }
            else
            {
                currentChunk = new Chunk(this, buffer);
            }
        }

        public ByteBuffer get(int size, boolean direct)
        {
            metrics.requests.mark();

            if (!direct)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Requested heap buffer for {} bytes, allocating on heap", size);
                return allocate(size, false);
            }

            if (size > GlobalPool.BUFFER_SIZE)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Requested buffer size {} is bigger than {}, allocating directly", size, GlobalPool.BUFFER_SIZE);
                return allocate(size, true);
            }

            if (currentChunk.isRecycled() || !currentChunk.hasCapacity(size))
                grabChunk();

            if (currentChunk.hasCapacity(size))
                return currentChunk.get(size);

            if (logger.isTraceEnabled())
                logger.trace("Requested buffer size {} has been allocated directly due to lack of capacity", size);

            return allocate(size, true);
        }

        private ByteBuffer allocate(int size, boolean direct)
        {
            metrics.misses.mark();
            return direct
                    ? Allocator.direct(size).allocate()
                    : Allocator.heap(size).allocate();
        }

        public void put(ByteBuffer buffer)
        {
            Chunk chunk = Chunk.get(buffer);
            if (chunk == null || chunk.isRecycled())
            {
                if (logger.isTraceEnabled())
                    logger.trace("Chunk for {} is not available", buffer);

                Allocator.deallocate(buffer);
                return;
            }

            if (chunk == EMPTY_CHUNK)
            { //trivial case
                assert buffer.capacity() == 0;
                return;
            }

            chunk.put(buffer, this);

            if (chunk != currentChunk)
                recycle(chunk);
        }

        private void recycle(Chunk chunk)
        {
            if (chunk.isFree() && !chunk.isRecycled())
            {
                boolean recycled = chunk.recycle(pool);
                if (logger.isTraceEnabled())
                    if (recycled)
                        logger.trace("Chunk {} was recycled", chunk);
                    else
                        logger.trace("Chunk {} was not recycled", chunk);
            }
        }
    }

    /**
     * This class takes care of allocating and deallocating byte buffers,
     * which can be page aligned if requested.
     */
    private final static class Allocator
    {
        private final int size ;
        private final boolean direct;
        private final boolean aligned;

        public static Allocator aligned(int size)
        {
            return new Allocator(size, true, true);
        }

        public static Allocator direct(int size)
        {
            return new Allocator(size, true, false);
        }

        public static Allocator heap(int size)
        {
            return new Allocator(size, false, false);
        }

        private Allocator(int size, boolean direct, boolean aligned)
        {
            if (size < 0)
                throw new IllegalArgumentException("allocation size must not be negative");

            this.size = size;
            this.direct = direct;
            this.aligned = aligned;
        }

        public ByteBuffer allocate()
        {
            return direct
                    ? allocateDirect()
                    : ByteBuffer.allocate(size);
        }

        private ByteBuffer allocateDirect()
        {
            return aligned
                    ? allocateAligned(size, MemoryUtil.pageSize())
                    : ByteBuffer.allocateDirect(size);
        }

        private ByteBuffer allocateAligned(int capacity, int align)
        {
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

        public static void deallocate(ByteBuffer buffer)
        {
            if (buffer.isDirect())
            {
                DirectBuffer db = (DirectBuffer)buffer;
                if (db.cleaner() != null && db.attachment() == null)
                { //slices have no cleaner and should not be cleaned, their parent should
                    FileUtils.clean(buffer);
                }
            }
        }
    }

    /**
     * A memory chunk, it takes a buffer and slices it
     * into smaller buffers.
     */
    final static class Chunk
    {
        // The minimum size of a returned buffer in bytes, @see roundUp and Slot.pack()
        private static final int MIN_BUFFER_SIZE = 64;

        static {
            assert Integer.bitCount(MIN_BUFFER_SIZE) == 1; // must be a power of 2
        }

        public final LocalPool owner;
        private final ByteBuffer slab;
        private long baseAddress;
        private final int capacity;
        private int offset;

        // Incremented when the owing pool releases a buffer, single thread update
        private int normalFree;

        // Incremented when another pool releases a buffer, multiple thread update
        private volatile int atomicFree;
        private static final AtomicIntegerFieldUpdater<Chunk> atomicFreeUpdater = AtomicIntegerFieldUpdater.newUpdater(Chunk.class, "atomicFree");

        static int NUM_FREE_SLOTS = 16; // the number of pages that fit in a buffer
        private short[] freeSlots = new short[NUM_FREE_SLOTS];
        private int freeSlotsIdx = 0;

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
            this.capacity = slab.capacity(); // because slab may be recycled before we disappear
            this.offset = 0;

            this.normalFree = this.capacity;
            this.atomicFree = 0;

            Arrays.fill(this.freeSlots, Slot.EMPTY_VALUE);
        }

        public static Chunk get(ByteBuffer buffer)
        {
            if (buffer.isDirect())
            {
                Object attachment = ((DirectBuffer)buffer).attachment();
                if (attachment != null && attachment instanceof Chunk)
                {
                    return (Chunk) attachment;
                }
            }
            return null;
        }

        public boolean isFree()
        {
            return free() >= capacity;
        }

        /** We increment the atomic free count by one when recycling, @see recycle() */
        public boolean isRecycled()
        {
            return free() == capacity + 1;
        }

        /** The total free size, includes lost free slots that could not be compacted */
        int free()
        {
            return normalFree + atomicFree;
        }

        @VisibleForTesting
        int offset()
        {
            return offset;
        }

        /** Return the slab to the global pool if we can atomically increment the atomic
         * free so that our combined free count (atomic and normal) is exactly capacity + 1.
         */
        public boolean recycle(GlobalPool globalPool)
        {
            int cur = atomicFree;
            int combined = cur + normalFree;
            if (combined == capacity && atomicFreeUpdater.compareAndSet(this, cur, cur + 1))
            {
                globalPool.recycle(slab);
                return true;
            }

            return false;
        }

        /** Checks the allocation capacity, that is the free size at the end of the chunk */
        public boolean hasCapacity(int size)
        {
            if (capacity - offset >= roundUp(size))
                return true;

            if (compactFreeSlots())
                return capacity - offset >= roundUp(size);
            else
                return false;
        }

        /** Return the next available slice of this size. If
         * we have exceeded the capacity we return an empty buffer.
         */
        public ByteBuffer get(int size)
        {
            if (size < 0)
                throw new IllegalArgumentException("Expected positive size");

            if (!hasCapacity(size))
                return EMPTY_BUFFER;

            slab.limit(offset + size);
            slab.position(offset);

            int roundedSize = roundUp(size);

            offset += roundedSize;
            assert offset <= capacity;

            normalFree -= roundedSize;
            assert normalFree >= 0;

            ByteBuffer ret = slab.slice();
            MemoryUtil.setAttachment(ret, slab, this);
            return ret;
        }

        /** Round the size to the next power of two with a minimum size of MIN_BUFFER_SIZE,
         * except for zero. This is required for the encoding of free slots, see Slot.pack().
         */
        private int roundUp(int v)
        {
            if (v == 0)
                return 0;

            if (v <= MIN_BUFFER_SIZE)
                return MIN_BUFFER_SIZE;

            if ((v & (v-1)) == 0)
                return v; // already a power of two

            // See http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
            // It works by copying the highest set bit to all of the lower bits,
            // and then adding one, which results in carries that set all of the lower
            // bits to 0 and one bit beyond the highest set bit to 1.

            //v--; // this decrements in case it is a power of two, commented out due to check above
            v |= v >> 1; // this sets highest 2 positions
            v |= v >> 2; // this sets highest 4 positions
            v |= v >> 4; // this sets highest 8 positions
            v |= v >> 8; // this sets highest 16 positions
            v |= v >> 16; // this sets highest 32 positions
            v++; // this increments by one to give back a power of two

            return v;
        }

        /** Release a buffer */
        public void put(ByteBuffer buffer, LocalPool pool)
        {
            long address = checkAddress(buffer);

            if (!MemoryUtil.setAttachment(buffer, this, slab))
                return;

            if (pool != owner)
                releaseDifferentPool(buffer);
            else
                releaseSamePool(buffer, address);
        }

        /** Check that the address is in the slab range and the offset
         * can be stored into an integer, else throw illegal argument exceptions.
         */
        private long checkAddress(ByteBuffer buffer)
        {
            long address = MemoryUtil.getAddress(buffer);

            if (address < baseAddress || address > (baseAddress + capacity))
                throw new IllegalArgumentException(String.format("Address is outside of the slab range : %d - %s", address, slab));

            if ((address - baseAddress) > Integer.MAX_VALUE)
                throw new IllegalArgumentException(String.format("Address offset is too big for an integer : %d", address - baseAddress));

            return address;
        }

        /** This is called by a thread local pool that has been mistakenly given
         * a byte buffer allocated by another thread local pool, we simply increment an atomic
         * counter so that we can be recycled at the end of our life and we do not
         * attempt to reuse the buffer.
         */
        public void releaseDifferentPool(ByteBuffer buffer)
        {
            logger.error("Byte buffer {} was released from different thread: {}, original thread was {}",
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
            if (owner != null)
            {
                Thread[] threads = new Thread[Thread.activeCount()];
                Thread.enumerate(threads);
                for (Thread t : threads)
                {
                    if (t.getId() == owner.threadId)
                        return t.getName();
                }
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

            normalFree += size;

            Slot slot = new Slot(position, size);
            if (appendToTheEnd(slot))
                return;

            short packed = slot.pack();
            if (packed == Slot.EMPTY_VALUE)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Dropping incoming free slot with pos {} and size {}, cannot pack it", position, size);
                return;
            }

            if (freeSlotsIdx == NUM_FREE_SLOTS)
            {
                if (compactFreeSlots() && appendToTheEnd(slot))
                    return;
            }

            if (freeSlotsIdx < NUM_FREE_SLOTS)
            {
                freeSlots[freeSlotsIdx++] = packed;
            }
            else
            {
                replaceSmallestFreeSlot(position, size);
            }
        }

        /**
         * See if we can append the free slot to the end by rolling
         * back the offset and therefore increasing the capacity.
         *
         * @return true if we appended the slot, false otherwise
         */
        private boolean appendToTheEnd(Slot slot)
        {
            if (slot.position == (offset - slot.size))
            {
                offset = slot.position;
                return true;
            }

            return false;
        }

        /**
         * A class encoding and decoding position and size into a short.
         */
        private final static class Slot
        {
            private final static short MASK = 0x0F; // bottom 4 bits for the size
            private final static short SHIFT = 4;
            public final static short EMPTY_VALUE = Short.MAX_VALUE; // so when compared empty values end up at the end

            public final int position;
            public final int size;

            public Slot(int position, int size)
            {
                this.position = position;
                this.size = size;
            }

            public short pack()
            {
                return pack(position, size);
            }

            public static short pack(int pos, int size)
            {
                short p = encodePosition(pos);
                short s = encodeSize(size);
                if (p == EMPTY_VALUE || s == EMPTY_VALUE)
                    return EMPTY_VALUE;

                return (short)((p << SHIFT) | (s & MASK));
            }

            public static Slot unpack(short val)
            {
                return new Slot(decodePosition((short)(val >>> SHIFT)), decodeSize((short)(val & MASK)));
            }

            /** The size is a power of 2 between Chunk.MIN_BUFFER_SIZE
             * (64) and GlobalPool.BUFFER_SIZE (64k) but in fact 64k
             * means the whole chunk is taken and the release would simply
             * be appended to the end so it's realistically 32k.
             * We encode the size by storing the bit position - 6 (64 = 2^6).
             * It will fit in 4 bits as there should be a total of 11 values.
             * @return
             */
            private static short encodeSize(int size)
            {
                if (((size & (size-1)) != 0) ||
                        size != 0 && size < Chunk.MIN_BUFFER_SIZE ||
                        size > GlobalPool.BUFFER_SIZE)
                {
                    logger.error("Cannot encode size {} : expected a power of two between {} and {}",
                                                                     size,
                                                                     Chunk.MIN_BUFFER_SIZE,
                                                                     GlobalPool.BUFFER_SIZE);
                    return EMPTY_VALUE;
                }

                int ret = Integer.numberOfTrailingZeros(size) - 6;

                assert (ret & 0xFFFFFFF0) == 0; // only first 4 bits should be set
                return (short)ret;
            }

            private static int decodeSize(short size)
            {
                return 1 << (size + 6);
            }

            /** The position is in multiples of Chunk.MIN_BUFFER_SIZE (64)
             * from zero to GlobalPool.BUFFER_SIZE (64k) so we encode it
             * by dividing by Chunk.MIN_BUFFER_SIZE and then we are sure
             * it will only take 10 bits (2^16 (64k) / 2^10 (64) = 2^10).
             */
            private static short encodePosition(int pos)
            {
                if ((pos & (Chunk.MIN_BUFFER_SIZE - 1)) != 0 || // x & (y-1) equals x % y if y is a power of two
                        pos > GlobalPool.BUFFER_SIZE)
                {
                    logger.error("Cannot encode position : {} is not a multiple of {} or it's bigger than {}",
                                 pos,
                                 Chunk.MIN_BUFFER_SIZE,
                                 GlobalPool.BUFFER_SIZE);
                    return EMPTY_VALUE;
                }

                int ret = pos / Chunk.MIN_BUFFER_SIZE;

                assert (ret & 0xFFFFFC00) == 0; // only first 10 bits should be set
                return (short)ret;
            }

            private static int decodePosition(short pos)
            {
                return pos * Chunk.MIN_BUFFER_SIZE;
            }
        }

        /**
         * Append free slots to the end, is possible.
         * @return true if at least one slot was appended, false otherwise.
         */
        boolean compactFreeSlots()
        {
            if (freeSlotsIdx == 0)
                return false;

            Arrays.sort(freeSlots);

            int i = freeSlotsIdx - 1;
            while (i >= 0)
            {
                Slot slot = Slot.unpack(freeSlots[i]);
                if (!appendToTheEnd(slot))
                    break;
                else
                    freeSlots[i] = Slot.EMPTY_VALUE;

                i--;
            }

            boolean compactedSome = i < (freeSlotsIdx - 1);
            freeSlotsIdx = i + 1;

            if (logger.isTraceEnabled())
                logger.trace("{} free slots compacted, current free slot index and offset : {}, {}",
                             compactedSome ? "Some" : "No", freeSlotsIdx, offset);

            return compactedSome;
        }

        /**
         * Replace the smallest size free slot with the incoming slot,
         * if slots have the same size replace the one with the smallest
         * position since higher positions have a better chance of being
         * compacted at the end.
         */
        private void replaceSmallestFreeSlot(int position, int size)
        {
            Slot slot = new Slot(position, size);
            int idx = -1;

            for (int i = 0; i < freeSlotsIdx; i++)
            {
                Slot s = Slot.unpack(freeSlots[i]);
                if (s.size < slot.size || (s.size == slot.size && s.position < slot.position))
                {
                    slot = s;
                    idx = i;
                }
            }

            if (idx != -1)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Dropping free slot at index {} with pos {} and size {}", idx, slot.position, slot.size);

                freeSlots[idx] = Slot.pack(position, size);
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Dropping incoming free slot with pos {} and size {}", slot.position, slot.size);
            }
        }

        @Override
        public String toString()
        {
            return String.format("[slab %s, offset %d, capacity %d, free %d]", slab, offset, capacity, free());
        }
    }
}
