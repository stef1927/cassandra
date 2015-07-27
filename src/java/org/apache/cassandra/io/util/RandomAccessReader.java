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
import java.nio.MappedByteBuffer;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

public class RandomAccessReader extends AbstractDataInput implements FileDataInput
{
    public static final int DEFAULT_BUFFER_SIZE = 4096;

    // the IO channel to the file, we do not own a reference to this due to
    // performance reasons (CASSANDRA-9379) so it's up to the owner of the RAR to
    // ensure that the channel stays open and that it is closed afterwards
    protected final ChannelProxy channel;

    // optional mmapped buffers for the channel, the key is the channel position
    protected final TreeMap<Long, MappedByteBuffer> segments;

    // an optional rate limiter to limit rebuffering rate, may be null
    protected final RateLimiter limiter;

    // this can be overridden at construction to a value shorter than the true length of the file;
    // if so, it acts as an imposed limit on reads, rather than a convenience property
    // we can cache file length in read-only mode
    private final long fileLength;

    // buffer which will cache file blocks or mirror mmapped segments
    protected ByteBuffer buffer;

    // offset from the beginning of the file
    protected long bufferOffset;

    // offset of the last file mark
    protected long markedPointer;

    protected RandomAccessReader(Builder builder)
    {
        this.channel = builder.channel;
        this.segments = builder.segments;
        this.limiter = builder.limiter;
        this.fileLength = builder.overrideLength <= 0 ? channel.size() : builder.overrideLength;

        if (segments.isEmpty())
        {
            if (builder.bufferSize <= 0)
                throw new IllegalArgumentException("bufferSize must be positive");

            buffer = allocateBuffer(getBufferSize(builder.bufferSize), builder.bufferType);
            buffer.limit(0);
        }
        else
        {
            buffer = segments.get(0);
        }
    }

    /** The buffer size is typically already page aligned but if that is not the case
     * make sure that it is a multiple of the page size, 4096.
     * */
    protected int getBufferSize(int size)
    {
        if ((size & ~4095) != size)
        { // should already be a page size multiple but if that's not case round it up
            size = (size + 4095) & ~4095;
        }
        return size;
    }

    protected ByteBuffer allocateBuffer(int size, BufferType bufferType)
    {
        return BufferPool.get(size, bufferType);
    }

    public ChannelProxy getChannel()
    {
        return channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        if (limiter != null)
            limiter.acquire(buffer.capacity());

        if (!segments.isEmpty())
        {
            reBufferMmap();
        }
        else
        {
            reBufferStandard();
        }
    }

    protected void reBufferStandard()
    {
        bufferOffset += buffer.position();
        buffer.clear();
        assert bufferOffset < fileLength;

        long position = bufferOffset;
        long limit = bufferOffset;

        long pageAligedPos = position & ~4095;
        // Because the buffer capacity is a multiple of the page size, we read less
        // the first time and then we should read at page boundaries only,
        // unless the user seeks elsewhere
        long upperLimit = Math.min(fileLength, pageAligedPos + buffer.capacity());
        buffer.limit((int)(upperLimit - position));
        while (buffer.hasRemaining() && limit < upperLimit)
        {
            int n = channel.read(buffer, position);
            if (n < 0)
                break;
            position += n;
            limit = bufferOffset + buffer.position();
        }
        if (limit > fileLength)
            buffer.position((int)(fileLength - bufferOffset));
        buffer.flip();
    }

    protected void reBufferMmap()
    {
        bufferOffset += buffer.position();
        assert bufferOffset < fileLength;

        Map.Entry<Long, MappedByteBuffer> entry = segments.floorEntry(bufferOffset);
        if (entry != null)
        {
            bufferOffset = entry.getKey();
            buffer = entry.getValue().duplicate();
            buffer.flip();
        }
        else
        {
            throw new IllegalStateException("Segment not found for position " + bufferOffset);
        }
    }

    @Override
    public long getFilePointer()
    {
        return current();
    }

    protected long current()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public String getPath()
    {
        return channel.filePath();
    }

    public int getTotalBufferSize()
    {
        //This may NPE so we make a ref
        //https://issues.apache.org/jira/browse/CASSANDRA-7756
        ByteBuffer ref = buffer;
        return ref != null ? ref.capacity() : 0;
    }

    public void reset()
    {
        seek(markedPointer);
    }

    public long bytesPastMark()
    {
        long bytes = current() - markedPointer;
        assert bytes >= 0;
        return bytes;
    }

    public FileMark mark()
    {
        markedPointer = current();
        return new BufferedRandomAccessFileMark(markedPointer);
    }

    public void reset(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        seek(((BufferedRandomAccessFileMark) mark).pointer);
    }

    public long bytesPastMark(FileMark mark)
    {
        assert mark instanceof BufferedRandomAccessFileMark;
        long bytes = current() - ((BufferedRandomAccessFileMark) mark).pointer;
        assert bytes >= 0;
        return bytes;
    }

    /**
     * @return true if there is no more data to read
     */
    public boolean isEOF()
    {
        return getFilePointer() == length();
    }

    public long bytesRemaining()
    {
        return length() - getFilePointer();
    }

    @Override
    public void close()
    {
	    //make idempotent
        if (buffer == null)
            return;

        bufferOffset += buffer.position();
        BufferPool.put(buffer);
        buffer = null;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + channel + "')";
    }

    /**
     * Class to hold a mark to the position of the file
     */
    protected static class BufferedRandomAccessFileMark implements FileMark
    {
        final long pointer;

        public BufferedRandomAccessFileMark(long pointer)
        {
            this.pointer = pointer;
        }
    }

    @Override
    public void seek(long newPosition)
    {
        if (newPosition < 0)
            throw new IllegalArgumentException("new position should not be negative");

        if (buffer == null)
            throw new IllegalStateException("Attempted to seek in a closed RAR");

        if (newPosition >= length()) // it is save to call length() in read-only mode
        {
            if (newPosition > length())
                throw new IllegalArgumentException(String.format("Unable to seek to position %d in %s (%d bytes) in read-only mode",
                                                             newPosition, getPath(), length()));
            buffer.limit(0);
            bufferOffset = newPosition;
            return;
        }

        if (newPosition >= bufferOffset && newPosition < bufferOffset + buffer.limit())
        {
            buffer.position((int) (newPosition - bufferOffset));
            return;
        }
        // Set current location to newPosition and clear buffer so reBuffer calculates from newPosition
        bufferOffset = newPosition;
        buffer.clear();
        reBuffer();
        assert current() == newPosition;
    }

    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read()
    {
        if (buffer == null)
            throw new AssertionError("Attempted to read from closed RAR");

        if (isEOF())
            return -1; // required by RandomAccessFile

        if (!buffer.hasRemaining())
            reBuffer();

        return (int)buffer.get() & 0xff;
    }

    @Override
    public int read(byte[] buffer)
    {
        return read(buffer, 0, buffer.length);
    }

    @Override
    // -1 will be returned if there is nothing to read; higher-level methods like readInt
    // or readFully (from RandomAccessFile) will throw EOFException but this should not
    public int read(byte[] buff, int offset, int length)
    {
        if (buffer == null)
            throw new IllegalStateException("Attempted to read from closed RAR");

        if (length == 0)
            return 0;

        if (isEOF())
            return -1;

        if (!buffer.hasRemaining())
            reBuffer();

        int toCopy = Math.min(length, buffer.remaining());
        buffer.get(buff, offset, toCopy);
        return toCopy;
    }

    public ByteBuffer readBytes(int length) throws EOFException
    {
        assert length >= 0 : "buffer length should not be negative: " + length;

        if (buffer == null)
            throw new IllegalStateException("Attempted to read from closed RAR");

        try
        {
            ByteBuffer result = ByteBuffer.allocate(length);
            while (result.hasRemaining())
            {
                if (isEOF())
                    throw new EOFException();
                if (!buffer.hasRemaining())
                    reBuffer();
                ByteBufferUtil.put(buffer, result);
            }
            result.flip();
            return result;
        }
        catch (EOFException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new FSReadError(e, channel.toString());
        }
    }

    public long length()
    {
        return fileLength;
    }

    public long getPosition()
    {
        return bufferOffset + (buffer == null ? 0 : buffer.position());
    }

    public long getPositionLimit()
    {
        return length();
    }

    public static class Builder
    {
        protected final ChannelProxy channel;
        protected long overrideLength;
        protected int bufferSize;
        protected BufferType bufferType;
        protected TreeMap<Long, MappedByteBuffer> segments;
        protected RateLimiter limiter;

        public Builder(ChannelProxy channel)
        {
            this.channel = channel;
            this.overrideLength = -1L;
            this.bufferSize = DEFAULT_BUFFER_SIZE;
            this.bufferType = BufferType.OFF_HEAP;
            this.segments = new TreeMap<>();
            this.limiter = null;
        }

        public Builder overrideLength(long overrideLength)
        {
            this.overrideLength = overrideLength;
            return this;
        }

        public Builder bufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        public Builder segments(Map<Long, MappedByteBuffer> segments)
        {
            this.segments.putAll(segments);
            return this;
        }

        public Builder limiter(RateLimiter limiter)
        {
            this.limiter = limiter;
            return this;
        }

        public RandomAccessReader build()
        {
            return new RandomAccessReader(this);
        }

        public RandomAccessReader buildWithChannel()
        {
            return new RandomAccessReaderWithChannel(this);
        }
    }

    // A wrapper of the RandomAccessReader that closes the channel when done.
    // For performance reasons RAR does not increase the reference count of
    // a channel but assumes the owner will keep it open and close it,
    // see CASSANDRA-9379, this thin class is just for those cases where we do
    // not have a shared channel.
    private static class RandomAccessReaderWithChannel extends RandomAccessReader
    {
        RandomAccessReaderWithChannel(Builder builder)
        {
            super(builder);
        }

        @Override
        public void close()
        {
            try
            {
                super.close();
            }
            finally
            {
                channel.close();
            }
        }
    }

    @SuppressWarnings("resource")
    public static RandomAccessReader open(File file)
    {
        return new Builder(new ChannelProxy(file)).buildWithChannel();
    }

    public static RandomAccessReader open(ChannelProxy channel)
    {
        return new Builder(channel).build();
    }
}
