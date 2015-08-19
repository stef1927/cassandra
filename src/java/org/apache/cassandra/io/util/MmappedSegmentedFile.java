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

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.utils.JVMStabilityInspector;

public class MmappedSegmentedFile extends SegmentedFile
{
    private static final Logger logger = LoggerFactory.getLogger(MmappedSegmentedFile.class);

    // in a perfect world, MAX_SEGMENT_SIZE would be final, but we need to test with a smaller size to stay sane.
    public static long MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

    /**
     * Sorted map of segment offsets and MappedByteBuffers for segments. If mmap is completely disabled, or if the
     * segment would be too long to mmap, the value for an offset will be null, indicating that we need to fall back
     * to a RandomAccessFile.
     */
    private final Map<Long, ByteBuffer> segments;

    public MmappedSegmentedFile(ChannelProxy channel, int bufferSize, long length, Map<Long, ByteBuffer> segments)
    {
        super(new Cleanup(channel, segments), channel, bufferSize, length);
        this.segments = segments;
    }

    private MmappedSegmentedFile(MmappedSegmentedFile copy)
    {
        super(copy);
        this.segments = copy.segments;
    }

    public MmappedSegmentedFile sharedCopy()
    {
        return new MmappedSegmentedFile(this);
    }

    public RandomAccessReader createReader()
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .segments(segments)
               .build();
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new RandomAccessReader.Builder(channel)
               .overrideLength(length)
               .bufferSize(bufferSize)
               .segments(segments)
               .limiter(limiter)
               .build();
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        private final Map<Long, ByteBuffer> segments;
        protected Cleanup(ChannelProxy channel, Map<Long, ByteBuffer> segments)
        {
            super(channel);
            this.segments = segments;
        }

        public void tidy()
        {
            super.tidy();

            if (!FileUtils.isCleanerAvailable())
                return;

        /*
         * Try forcing the unmapping of segments using undocumented unsafe sun APIs.
         * If this fails (non Sun JVM), we'll have to wait for the GC to finalize the mapping.
         * If this works and a thread tries to access any segment, hell will unleash on earth.
         */
            try
            {
                for (ByteBuffer segment : segments.values())
                {
                    if (segment == null)
                        continue;
                    FileUtils.clean(segment);
                }
                logger.debug("All segments have been unmapped successfully");
            }
            catch (Exception e)
            {
                JVMStabilityInspector.inspectThrowable(e);
                // This is not supposed to happen
                logger.error("Error while unmapping segments", e);
            }
        }
    }

    /**
     * Overrides the default behaviour to create segments of a maximum size.
     */
    static class Builder extends SegmentedFile.Builder
    {
        public Builder()
        {
            super();
        }

        public SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            long length = overrideLength > 0 ? overrideLength : channel.size();
            // create the segments
            return new MmappedSegmentedFile(channel, bufferSize, length, createSegments(channel, length));
        }

        private Map<Long, ByteBuffer> createSegments(ChannelProxy channel, long length)
        {
            Map<Long, ByteBuffer> segments = new TreeMap<>();

            long pos = 0;
            while (pos < length)
            {
                long size = Math.min(MAX_SEGMENT_SIZE, channel.size() - pos);
                segments.put(pos, channel.map(FileChannel.MapMode.READ_ONLY, pos, size));
                pos += size;
            }

            return segments;
        }
    }
}
