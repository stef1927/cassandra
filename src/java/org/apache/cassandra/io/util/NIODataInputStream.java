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

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.utils.vint.VIntCoding;

import com.google.common.base.Preconditions;

/**
 * Rough equivalent of BufferedInputStream and DataInputStream wrapping the input stream of a File or Socket
 * Created to work around the fact that when BIS + DIS delegate to NIO for socket IO they will allocate large
 * thread local direct byte buffers when a large array is used to read.
 *
 * There may also be some performance improvement due to using a DBB as the underlying buffer for IO and the removal
 * of some indirection and delegation when it comes to reading out individual values, but that is not the goal.
 *
 * Closing NIODataInputStream will invoke close on the ReadableByteChannel provided at construction.
 *
 * NIODataInputStream is not thread safe.
 */
public class NIODataInputStream extends InputStream implements DataInputPlus, Closeable
{
    protected final ReadableByteChannel channel;
    protected ByteBuffer buffer;

    /*
     *  Used when wrapping a fixed buffer of data instead of a channel. Should never attempt
     *  to read from it.
     */
    private static final ReadableByteChannel emptyReadableByteChannel = new ReadableByteChannel()
    {

        @Override
        public boolean isOpen()
        {
            return true;
        }

        @Override
        public void close()
        {
        }

        @Override
        public int read(ByteBuffer dst)
        {
            throw new AssertionError();
        }

    };

    private static ByteBuffer makeBuffer(int bufferSize)
    {
        Preconditions.checkArgument(bufferSize >= 9, "Buffer size must be large enough to accomadate a varint");
        ByteBuffer buffer = ByteBuffer.allocateDirect(bufferSize);
        buffer.position(0);
        buffer.limit(0);

        return buffer;
    }

    public NIODataInputStream(ReadableByteChannel channel, ByteBuffer buffer)
    {
        Preconditions.checkNotNull(channel);
        Preconditions.checkArgument(buffer == null || buffer.order() == ByteOrder.BIG_ENDIAN, "Buffer must have BIG ENDIAN byte ordering");

        this.channel = channel;
        this.buffer = buffer;
    }

    public NIODataInputStream(ReadableByteChannel channel, int bufferSize)
    {
        this(channel, makeBuffer(bufferSize));
    }

    protected NIODataInputStream(ByteBuffer buffer, boolean duplicate)
    {
        this(emptyReadableByteChannel, duplicate ? buffer.duplicate() : buffer);
    }

    /*
     * The decision to duplicate or not really needs to conscious since it a real impact
     * in terms of thread safety so don't expose this constructor with an implicit default.
     */
    protected NIODataInputStream(ByteBuffer buf)
    {
        this(emptyReadableByteChannel, buf);
    }

    protected NIODataInputStream(ReadableByteChannel channel)
    {
        this(channel, null);
    }

    @Override
    public void readFully(byte[] b) throws IOException
    {
        readFully(b, 0, b.length);
    }


    @Override
    public void readFully(byte[] b, int off, int len) throws IOException
    {
        int copied = 0;
        while (copied < len)
        {
            int read = read(b, off + copied, len - copied);
            if (read < 0)
                throw new EOFException();
            copied += read;
        }
    }

    @Override
    public int read(byte b[], int off, int len) throws IOException {
        if (b == null)
            throw new NullPointerException();

        // avoid int overflow
        if (off < 0 || off > b.length || len < 0 || len > b.length - off)
            throw new IndexOutOfBoundsException();

        if (len == 0)
            return 0;

        int copied = 0;
        while (copied < len)
        {
            if (buffer.hasRemaining())
            {
                int toCopy = Math.min(len - copied, buffer.remaining());
                buffer.get(b, off + copied, toCopy);
                copied += toCopy;
            }
            else
            {
                reBuffer();
                if (!buffer.hasRemaining())
                    return copied == 0 ? -1 : copied;
            }
        }

        return copied;
    }

    /*
     * Refill the buffer.
     */
    protected void reBuffer() throws IOException
    {
        Preconditions.checkState(buffer.remaining() == 0);
        buffer.clear();

        while ((channel.read(buffer)) == 0) {}

        buffer.flip();
    }

    @DontInline
    private long readPrimitiveSlowly(int bytes) throws IOException
    {
        long result = 0;
        for (int i = 0; i < bytes; i++)
            result = (result << 8) | (readByte() & 0xFFL);
        return result;
    }

    @Override
    public int skipBytes(int n) throws IOException
    {
        int skipped = 0;

        while (skipped < n)
        {
            int skippedThisTime = (int)skip(n - skipped);
            if (skippedThisTime <= 0) break;
            skipped += skippedThisTime;
        }

        return skipped;
    }

    @Override
    public boolean readBoolean() throws IOException
    {
        return readByte() != 0;
    }

    @Override
    public byte readByte() throws IOException
    {
        if (!buffer.hasRemaining())
        {
            reBuffer();
            if (!buffer.hasRemaining())
                throw new EOFException();
        }

        return buffer.get();
    }

    @Override
    public int readUnsignedByte() throws IOException
    {
        return readByte() & 0xff;
    }

    @Override
    public short readShort() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getShort();
        else
            return (short) readPrimitiveSlowly(2);
    }

    @Override
    public int readUnsignedShort() throws IOException
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public char readChar() throws IOException
    {
        if (buffer.remaining() >= 2)
            return buffer.getChar();
        else
            return (char) readPrimitiveSlowly(2);
    }

    @Override
    public int readInt() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getInt();
        else
            return (int) readPrimitiveSlowly(4);
    }

    @Override
    public long readLong() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getLong();
        else
            return readPrimitiveSlowly(8);
    }

    public long readVInt() throws IOException
    {
        return VIntCoding.decodeZigZag64(readUnsignedVInt());
    }

    public long readUnsignedVInt() throws IOException
    {
        //If 9 bytes aren't available use the slow path in VIntCoding
        if (buffer.remaining() < 9)
            return VIntCoding.readUnsignedVInt(this);

        byte firstByte = buffer.get();

        //Bail out early if this is one byte, necessary or it fails later
        if (firstByte >= 0)
            return firstByte;

        int extraBytes = VIntCoding.numberOfExtraBytesToRead(firstByte);

        int position = buffer.position();
        int extraBits = extraBytes * 8;

        long retval = buffer.getLong(position);
        if (buffer.order() == ByteOrder.LITTLE_ENDIAN)
            retval = Long.reverseBytes(retval);
        buffer.position(position + extraBytes);

        // truncate the bytes we read in excess of those we needed
        retval >>>= 64 - extraBits;
        // remove the non-value bits from the first byte
        firstByte &= VIntCoding.firstByteValueMask(extraBytes);
        // shift the first byte up to its correct position
        retval |= (long) firstByte << extraBits;
        return retval;
    }

    @Override
    public float readFloat() throws IOException
    {
        if (buffer.remaining() >= 4)
            return buffer.getFloat();
        else
            return Float.intBitsToFloat((int)readPrimitiveSlowly(4));
    }

    @Override
    public double readDouble() throws IOException
    {
        if (buffer.remaining() >= 8)
            return buffer.getDouble();
        else
            return Double.longBitsToDouble(readPrimitiveSlowly(8));
    }

    @Override
    public String readLine() throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() throws IOException
    {
        return DataInputStream.readUTF(this);
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public int read() throws IOException
    {
        return readUnsignedByte();
    }

    @Override
    public int available() throws IOException
    {
        if (channel instanceof SeekableByteChannel)
        {
            SeekableByteChannel sbc = (SeekableByteChannel) channel;
            long remainder = Math.max(0, sbc.size() - sbc.position());
            return (remainder > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)(remainder + buffer.remaining());
        }
        return buffer.remaining();
    }

    @Override
    public void reset() throws IOException
    {
        throw new IOException("mark/reset not supported");
    }

    @Override
    public boolean markSupported()
    {
        return false;
    }
}
