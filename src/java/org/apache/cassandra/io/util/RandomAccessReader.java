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

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
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
    protected final Channel channel;

    // optional mmapped buffers for the channel, the key is the channel position
    protected final TreeMap<Long, ByteBuffer> segments;

    // an optional rate limiter to limit rebuffering rate, may be null
    protected final RateLimiter limiter;

    // the file length, this can be overridden at construction to a value shorter
    // than the true length of the file; if so, it acts as an imposed limit on reads,
    // required when opening sstables early not to read past the mark
    private final long fileLength;

    // the buffer size for buffered readers
    private final int bufferSize;

    // the buffer type for buffered readers
    private final BufferType bufferType;

    // buffer which will cache file blocks or mirror mmapped segments
    protected ByteBuffer buffer;

    // when this is true a call to releaseBuffer will release the buffer
    // else we assume the buffer is a mmapped segment
    private boolean hasAllocatedBuffer;

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
        this.bufferSize = getBufferSize(builder.bufferSize);
        this.bufferType = builder.bufferType;

        if (builder.bufferSize <= 0)
            throw new IllegalArgumentException("bufferSize must be positive");

        if (segments.isEmpty())
            buffer = allocateBuffer(getBufferSize(builder.bufferSize));
        else
            buffer = segments.firstEntry().getValue().slice();

        // this will ensure a rebuffer so we read at the correct position after seeking
        // especially important if the first segment is missing
        buffer.limit(0);
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

    protected ByteBuffer allocateBuffer(int size)
    {
        hasAllocatedBuffer = true;
        return BufferPool.get(size, bufferType);
    }

    protected void releaseBuffer()
    {
        if (hasAllocatedBuffer)
        {
            BufferPool.put(buffer);
            hasAllocatedBuffer = false;
        }

        buffer = null;
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
        assert bufferOffset < fileLength;

        buffer.clear();
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
        long position = bufferOffset + buffer.position();
        assert position < fileLength;

        releaseBuffer();

        Map.Entry<Long, ByteBuffer> entry = segments.floorEntry(position);
        if (entry == null || (position >= (entry.getKey() + entry.getValue().capacity())))
        {
            Map.Entry<Long, ByteBuffer> nextEntry = segments.ceilingEntry(position);
            if (nextEntry != null)
                buffer = allocateBuffer(Math.min(bufferSize, Ints.checkedCast(nextEntry.getKey() - position)));
            else
                buffer = allocateBuffer(bufferSize);

            bufferOffset = position;
            reBufferStandard();
            return;
        }

        bufferOffset = entry.getKey();
        buffer = entry.getValue().slice();
        buffer.position(Ints.checkedCast(position - bufferOffset));
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
        releaseBuffer();
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

    public interface Channel
    {
        String filePath();

        int read(ByteBuffer buffer, long position);

        long size();

        void close();
    }

    public static class EmptyChannel implements Channel
    {
        private final String filePath;
        private final int size;

        public EmptyChannel(String filePath, int size)
        {
            this.filePath = filePath;
            this.size = size;
        }

        public String filePath()
        {
            return filePath;
        }

        public int read(ByteBuffer buffer, long position)
        {
            throw new IllegalStateException("Unsupported operation at position " + position);
        }

        public long size()
        {
            return size;
        }

        public void close()
        {

        }
    }

    public static class Builder
    {
        // The NIO file channel or an empty channel
        protected final Channel channel;

        // We override the file length when we open sstables early, so that we do not
        // read past the early mark
        protected long overrideLength;

        // The size of the buffer for buffered readers
        protected int bufferSize;

        // The type of the buffer for buffered readers
        protected BufferType bufferType;

        // The mmap segments for mmap readers
        protected TreeMap<Long, ByteBuffer> segments;

        // An optional limiter to limit the rebuffering rate, may be null
        protected RateLimiter limiter;

        public Builder(Channel channel)
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

        public Builder segments(Map<Long, ByteBuffer> segments)
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

    /**
     * Open a RandomAccessReader for a single buffer at a specific offset.
     * Fake a file via an empty channel and map the only buffer to a
     * segment at the specified offset. Then seek to this offset.
     * Any reads before the offset or after offset + buffer.capacity() will
     * result in an exception throw by the EmptyChannel read() method.
     */
    public static RandomAccessReader open(ByteBuffer buffer, String filePath, long offset)
    {
        TreeMap<Long, ByteBuffer> segments = new TreeMap<>();
        segments.put(offset, buffer);
        RandomAccessReader ret = new Builder(new EmptyChannel(filePath, Ints.checkedCast(offset + buffer.capacity())))
                                 .segments(segments)
                                 .build();
        ret.seek(offset);
        return ret;
    }
}
