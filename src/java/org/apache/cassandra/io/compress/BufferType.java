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
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.NativeMemoryMetrics;
import org.apache.cassandra.utils.UnsafeByteBufferAccess;
import org.apache.cassandra.utils.UnsafeMemoryAccess;

public enum BufferType
{
    ON_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size);
        }
    },
    OFF_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }
    },
    OFF_HEAP_ALIGNED
    {
        public ByteBuffer allocate(int size)
        {
            return allocateDirectAligned(size);
        }
    };

    public abstract ByteBuffer allocate(int size);

    public static BufferType typeOf(ByteBuffer buffer)
    {
        return buffer.isDirect()
                ? ((UnsafeByteBufferAccess.getAddress(buffer) & -UnsafeMemoryAccess.pageSize()) == 0
                ? OFF_HEAP_ALIGNED
                : OFF_HEAP)
                : ON_HEAP;
    }

    /**
     * Allocate a buffer of the current type, write the content to it and return it.
     * The buffer is repositioned at the beginning, and it's ready for reading.
     *
     * @param content - the content to write
     *
     * @return a byte buffer with the specified content written in it
     */
    public ByteBuffer withContent(byte[] content)
    {
        ByteBuffer ret = allocate(content.length);
        ret.put(content);
        ret.rewind();
        return ret;
    }

    /**
     * For compression we always use off heap byte buffers, even if some compressors
     * are more optimized for ON HEAP buffers. This is because the chunk cache requires
     * off heap buffers, see DB-1584 and DSP-15245 for more details.
     *
     * @return the buffer types that can be used for compression
     */
    public static BufferType[] supportedForCompression()
    {
        return new BufferType[] { OFF_HEAP, OFF_HEAP_ALIGNED };
    }

    /**
     * Unless we need alignment as well, for compression we prefer just plain off heap.
     *
     *  @return the preferred type to be used for compression
     */
    public static BufferType preferredForCompression()
    {
        return OFF_HEAP;
    }

    private static ByteBuffer allocateDirectAligned(int capacity)
    {
        NativeMemoryMetrics.instance.totalAlignedAllocations.mark();

        int align = UnsafeMemoryAccess.pageSize();

        if (capacity < NativeMemoryMetrics.smallBufferThreshold)
            NativeMemoryMetrics.instance.smallAlignedAllocations.mark();

        if (Integer.bitCount(align) != 1)
            throw new IllegalArgumentException("Alignment must be a power of 2");

        ByteBuffer buffer = ByteBuffer.allocateDirect(capacity + align);
        long address = UnsafeByteBufferAccess.getAddress(buffer);
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

        // Mark the returned slice as a buffer with a cleanable parent since we want to release the direct
        // memory when the slice is released
        return FileUtils.SlicedBufferCleaner.mark(buffer.slice());
    }

}
