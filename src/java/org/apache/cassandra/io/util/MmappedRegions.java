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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

public class MmappedRegions extends SharedCloseableImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedRegions.class);

    // in a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size to stay sane.
    public static int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    // when we need to grow the allow the add this number of regions
    static final int REGION_ALLOC_SIZE = 15;

    /** The state is shared with the tidier and it contains all the regions and does the actual mapping */
    private final State state;

    private MmappedRegions(ChannelProxy channel)
    {
        this(new State(channel));
    }

    private MmappedRegions(State state)
    {
        super(new Tidier(state));
        this.state = state;
    }

    /** See comment in State(State original) */
    private MmappedRegions(MmappedRegions original)
    {
        super(original);
        this.state = new State(original.state);
    }

    public static MmappedRegions empty(ChannelProxy channel)
    {
        return new MmappedRegions(channel);
    }

    public static MmappedRegions map(ChannelProxy channel, CompressionMetadata metadata)
    {
        MmappedRegions ret = new MmappedRegions(channel);
        ret.create(metadata);
        return ret;
    }

    /** See comment in State(State original) */
    public MmappedRegions sharedCopy()
    {
        return new MmappedRegions(this);
    }

    /** See comment in State(State original) */
    public MmappedRegions snapshot()
    {
        return sharedCopy();
    }

    public MmappedRegions extend(long length)
    {
        if (length < 0)
            throw new IllegalArgumentException("Length must not be negative");

        if (length <= state.length)
            return this;

        state.length = length;
        long pos = state.getPosition();
        while (pos < length)
        {
            long size = Math.min(MAX_SEGMENT_SIZE, length - pos);
            state.add(pos, size);
            pos += size;
        }

        return this;
    }

    private void create(CompressionMetadata metadata)
    {
        long offset = 0;
        long lastSegmentOffset = 0;
        long segmentSize = 0;

        while (offset < metadata.dataLength)
        {
            CompressionMetadata.Chunk chunk = metadata.chunkFor(offset);

            //Reached a new mmap boundary
            if (segmentSize + chunk.length + 4 > MAX_SEGMENT_SIZE)
            {
                if (segmentSize > 0)
                {
                    state.add(lastSegmentOffset, segmentSize);
                    lastSegmentOffset += segmentSize;
                    segmentSize = 0;
                }
            }

            segmentSize += chunk.length + 4; //checksum
            offset += metadata.chunkLength();
        }

        if (segmentSize > 0)
            state.add(lastSegmentOffset, segmentSize);

        state.length = lastSegmentOffset + segmentSize;
    }

    public boolean isValid(ChannelProxy channel)
    {
        return state.isValid(channel);
    }

    public boolean isEmpty()
    {
        return state.isEmpty();
    }

    public Region floor(long position)
    {
        assert !isCleanedUp() : "Attempted to use closed region";
        return state.floor(position);
    }

    public static final class Region
    {
        public final long offset;
        public final ByteBuffer buffer;

        public Region(long offset, ByteBuffer buffer)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public long bottom()
        {
            return offset;
        }

        public long top()
        {
            return offset + buffer.capacity();
        }
    }


    private static final class State
    {
        /** The file channel */
        private final ChannelProxy channel;

        /** An array of region buffers, synchronized with offsets */
        private ByteBuffer[] buffers;

        /** An array of region offsets, synchronized with buffers */
        private long[] offsets;

        /** The maximum file length we have mapped */
        private long length;

        /** The index to the last region added */
        private int last;

        /** This is true when we are a copy of another state. In this case we can use existing regions but
         * we cannot modify them or create new regions or close them.
         */
        private final boolean isCopy;

        private State(ChannelProxy channel)
        {
            this.channel = channel.sharedCopy();
            this.buffers = new ByteBuffer[REGION_ALLOC_SIZE];
            this.offsets = new long[REGION_ALLOC_SIZE];
            this.length = 0;
            this.last = -1;
            this.isCopy = false;
        }

        /** We create a deep copy of the arrays for thread safety reasons.
         * We also store a flag indicating if we are a copy (isCopy). Copies
         * can only access existing regions, they cannot create new ones.
         * This is because MmappedRegions is reference counted, only the original
         * will be cleaned-up, therefore only the original can create new mapped
         * regions.
         *
         * @param original - the original state we are a copy of
         */
        private State(State original)
        {
            this.channel = original.channel;
            this.buffers = Arrays.copyOf(original.buffers, original.buffers.length);
            this.offsets = Arrays.copyOf(original.offsets, original.offsets.length);
            this.length = original.length;
            this.last = original.last;
            this.isCopy = true;
        }

        private boolean isEmpty()
        {
            return last < 0;
        }

        private boolean isValid(ChannelProxy channel)
        {
            return this.channel.filePath().equals(channel.filePath());
        }

        private Region floor(long position)
        {
            assert 0 <= position && position < length : String.format("%d >= %d", position, length);

            int idx = Arrays.binarySearch(offsets, 0, last +1, position);
            assert idx != -1 : String.format("Bad position %d for regions %s, last %d in %s", position, Arrays.toString(offsets), last, channel);
            if (idx < 0)
                idx = -(idx + 2); // round down to entry at insertion point

            return new Region(offsets[idx], buffers[idx]);
        }

        private long getPosition()
        {
            return last < 0 ? 0 : offsets[last] + buffers[last].capacity();
        }

        private void add(long pos, long size)
        {
            assert !isCopy : String.format("Cannot add a region to a copy (%s)", channel);

            ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, pos, size);

            ++last;

            if (last == offsets.length)
            {
                offsets = Arrays.copyOf(offsets, offsets.length + REGION_ALLOC_SIZE);
                buffers = Arrays.copyOf(buffers, buffers.length + REGION_ALLOC_SIZE);
            }

            offsets[last] = pos;
            buffers[last] = buffer;
        }

        private Throwable close(Throwable accumulate)
        {
            assert !isCopy : String.format("Cannot close a copy (%s)", channel);

            if (!FileUtils.isCleanerAvailable())
                return accumulate;

            /*
             * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
             * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
             * If this works and a thread tries to access any segment, hell will unleash on earth.
             */
            try
            {
                for (ByteBuffer buffer : buffers)
                {
                    if (buffer != null)
                    { // we could break when we encounter null,
                      // this is just a little bit of extra defensiveness
                        FileUtils.clean(buffer);
                    }
                }

                logger.debug("All segments have been unmapped successfully");

                channel.close();
                return accumulate;
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                // This is not supposed to happen
                logger.error("Error while unmapping segments", t);
                return Throwables.merge(accumulate, t);
            }
        }
    }

    public static final class Tidier implements RefCounted.Tidy
    {
        final State state;

        Tidier(State state)
        {
            this.state = state;
        }

        public String name()
        {
            return state.channel.filePath();
        }

        public void tidy()
        {
            try
            {
                Throwables.maybeFail(state.close(null));
            }
            catch (Exception e)
            {
                throw new FSReadError(e, state.channel.filePath());
            }
        }
    }

}
