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

import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

//TODO - add documentation and ref counting
public class MmappedRegions implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedRegions.class);

    // in a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size to stay sane.
    public static int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    // when we need to grow the allow the add this number of regions
    static final int REGION_ALLOC_SIZE = 15;
    private static final Region EMPTY_REGION = new Region(0, ByteBuffer.allocate(0));

    /** The file channel */
    private final ChannelProxy channel;

    /** An array of sorted mapped regions */
    private Region[] regions;

    /** The maximum file length we have mapped */
    private long length;

    /** The index to the last region added */
    private int last;

    private MmappedRegions(ChannelProxy channel)
    {
        this.channel = channel;
        this.regions = allocRegions(null);
        this.length = 0;
        this.last = -1;
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

    public MmappedRegions extend(long length)
    {
        if (length < 0)
            throw new IllegalArgumentException("Length must not be negative");

        if (length <= this.length)
            return this;

        this.length = length;
        long pos = lastRegion().top();
        while (pos < length)
        {
            long size = Math.min(MAX_SEGMENT_SIZE, length - pos);
            addRegion(pos, channel.map(FileChannel.MapMode.READ_ONLY, pos, size));
            pos += size;
        }

        //TODO - should we compact regions?

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
                    addRegion(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));
                    lastSegmentOffset += segmentSize;
                    segmentSize = 0;
                }
            }

            segmentSize += chunk.length + 4; //checksum
            offset += metadata.chunkLength();
        }

        if (segmentSize > 0)
            addRegion(lastSegmentOffset, channel.map(FileChannel.MapMode.READ_ONLY, lastSegmentOffset, segmentSize));

        this.length = lastSegmentOffset + segmentSize;
    }

    public boolean isSame(ChannelProxy channel)
    {
        return this.channel.filePath().equals(channel.filePath());
    }

    public boolean isEmpty()
    {
        return lastRegion() == EMPTY_REGION;
    }

    public Region floor(long position)
    {
        assert 0 <= position && position < length : String.format("%d >= %d", position, length);

        int idx = Arrays.binarySearch(regions, 0, last +1, new Region(position, null));
        assert idx != -1 : String.format("Bad position %d for regions %s", position, Arrays.toString(regions));
        if (idx < 0)
            idx = -(idx + 2); // round down to entry at insertion point

        return regions[idx];
    }

    public Throwable close(Throwable accumulate)
    {
        if (!FileUtils.isCleanerAvailable())
            return accumulate;

        /*
         * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
         * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
         * If this works and a thread tries to access any segment, hell will unleash on earth.
         */
        try
        {
            for (Region region : regions)
                FileUtils.clean(region.buffer);

            //TODO - this can be removed once we use ref counting
            last = -1;
            regions = null;

            logger.debug("All segments have been unmapped successfully");
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

    public void close() throws Exception
    {
        Throwables.maybeFail(close(null));
    }

    private Region lastRegion()
    {
        return last >= 0 ? regions[last] : EMPTY_REGION;
    }

    private void addRegion(long pos, ByteBuffer buffer)
    {
        if (++last == regions.length)
            regions = allocRegions(regions);

        regions[last] = new Region(pos, buffer);
    }

    private static Region[] allocRegions(Region[] existing)
    {
        Region[] ret;
        if (existing != null)
            ret = Arrays.copyOf(existing, existing.length + REGION_ALLOC_SIZE);
        else
            ret = new Region[REGION_ALLOC_SIZE];

        Arrays.fill(ret, existing == null ? 0 : existing.length, ret.length, EMPTY_REGION);
        return ret;
    }

    public static final class Region implements Comparable<Region>
    {
        public final long offset;
        public final ByteBuffer buffer;

        public Region(long offset, ByteBuffer buffer)
        {
            this.offset = offset;
            this.buffer = buffer;
        }

        public final int compareTo(Region that)
        {
            return (int) Math.signum(this.offset - that.offset);
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

}
