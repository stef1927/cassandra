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
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.memory.BufferPool;

public class RandomAccessReader extends NIODataInputStream implements FileDataInput
{
    public static final int DEFAULT_BUFFER_SIZE = 4096;

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

    // when this is true a call to releaseBuffer will release the buffer
    // else we assume the buffer is a mmapped segment
    private boolean hasAllocatedBuffer;

    // offset from the beginning of the file
    protected long bufferOffset;

    // offset of the last file mark
    protected long markedPointer;

    protected RandomAccessReader(Builder builder)
    {
        super(builder.channel);

        this.segments = builder.segments;
        this.limiter = builder.limiter;
        this.fileLength = builder.overrideLength <= 0 ? builder.channel.size() : builder.overrideLength;
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
        return BufferPool.get(size, bufferType).order(ByteOrder.BIG_ENDIAN);
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

    protected Channel fileChannel()
    {
        return (Channel) channel;
    }

    /**
     * Read data from file starting from current currentOffset to populate buffer.
     */
    protected void reBuffer()
    {
        if (isEOF())
            return;

        if (limiter != null)
            limiter.acquire(buffer.capacity());

        if (segments.isEmpty())
            reBufferStandard();
        else
            reBufferMmap();

        assert buffer.order() == ByteOrder.BIG_ENDIAN : "Buffer must have BIG ENDIAN byte ordering";
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
            int n = fileChannel().read(buffer, position);
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
        return fileChannel().filePath();
    }

    @Override
    public void reset() throws IOException
    {
        seek(markedPointer);
    }

    @Override
    public boolean markSupported()
    {
        return true;
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
    public int available() throws IOException
    {
        return Ints.checkedCast(bytesRemaining());
    }

    @Override
    public void close()
    {
	    //make idempotent
        if (buffer == null)
            return;

        bufferOffset += buffer.position();
        releaseBuffer();

        //For performance reasons we don't keep a reference to the file
        //channel so we don't close it, hence super.close() is not called.
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(" + "filePath='" + fileChannel().filePath() + "')";
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

    /**
     * Reads a line of text form the current position in this file. A line is
     * represented by zero or more characters followed by {@code '\n'}, {@code
     * '\r'}, {@code "\r\n"} or the end of file marker. The string does not
     * include the line terminating sequence.
     * <p>
     * Blocks until a line terminating sequence has been read, the end of the
     * file is reached or an exception is thrown.
     *
     * @return the contents of the line or {@code null} if no characters have
     *         been read before the end of the file has been reached.
     * @throws IOException
     *             if this file is closed or another I/O error occurs.
     */
    public final String readLine() throws IOException {
        StringBuilder line = new StringBuilder(80); // Typical line length
        boolean foundTerminator = false;
        long unreadPosition = -1;
        while (true) {
            int nextByte = read();
            switch (nextByte) {
                case -1:
                    return line.length() != 0 ? line.toString() : null;
                case (byte) '\r':
                    if (foundTerminator) {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    foundTerminator = true;
                    /* Have to be able to peek ahead one byte */
                    unreadPosition = getPosition();
                    break;
                case (byte) '\n':
                    return line.toString();
                default:
                    if (foundTerminator) {
                        seek(unreadPosition);
                        return line.toString();
                    }
                    line.append((char) nextByte);
            }
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

    public interface Channel extends ReadableByteChannel
    {
        String filePath();

        int read(ByteBuffer buffer, long position);

        long size();
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

        public int read(ByteBuffer dst) throws IOException
        {
            throw new IllegalStateException("Unsupported operation");
        }

        public int read(ByteBuffer buffer, long position)
        {
            throw new IllegalStateException("Unsupported operation at position " + position);
        }

        public long size()
        {
            return size;
        }

        public boolean isOpen()
        {
            return false;
        }

        public void close() { }
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
                FileUtils.closeQuietly(channel);
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
