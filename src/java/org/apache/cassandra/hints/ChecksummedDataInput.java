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
package org.apache.cassandra.hints;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.RebufferingInputStream;

/**
 * An {@link RandomAccessReader} wrapper that calctulates the CRC in place.
 *
 * Useful for {@link org.apache.cassandra.hints.HintsReader}, for example, where we must verify the CRC, yet don't want
 * to allocate an extra byte array just that purpose.
 *
 * In addition to calculating the CRC, allows to enforce a maximim known size. This is needed
 * so that {@link org.apache.cassandra.db.Mutation.MutationSerializer} doesn't blow up the heap when deserializing a
 * corrupted sequence by reading a huge corrupted length of bytes via
 * via {@link org.apache.cassandra.utils.ByteBufferUtil#readWithLength(java.io.DataInput)}.
 */
public final class ChecksummedDataInput extends RebufferingInputStream implements FileDataInput
{
    private final CRC32 crc;
    private final RandomAccessReader source;
    private int limit;

    private ChecksummedDataInput(RandomAccessReader source)
    {
        super(source);
        this.source = source;

        crc = new CRC32();
        limit = Integer.MAX_VALUE;
    }

    public static ChecksummedDataInput wrap(RandomAccessReader source)
    {
        return new ChecksummedDataInput(source);
    }

    public void resetCrc()
    {
        crc.reset();
    }

    public void resetLimit()
    {
        limit = Integer.MAX_VALUE;
    }

    public void limit(int newLimit)
    {
        limit = newLimit;
    }

    public String getPath()
    {
        return source.getPath();
    }

    public boolean isEOF()
    {
        return source.isEOF();
    }

    public long bytesRemaining()
    {
        return limit;
    }

    public int available()
    {
        return limit;
    }

    public int getCrc()
    {
        return (int) crc.getValue();
    }

    public void seek(long position)
    {
        source.seek(position);
    }

    public FileMark mark()
    {
        return source.mark();
    }

    public void reset(FileMark mark)
    {
        source.reset(mark);
    }

    public long bytesPastMark(FileMark mark)
    {
        return source.bytesPastMark(mark);
    }

    public long getFilePointer()
    {
        return source.getFilePointer();
    }

    public long getPosition()
    {
        return source.getPosition();
    }

    public ByteBuffer readBytes(int length) throws IOException
    {
        ByteBuffer ret = ByteBuffer.allocate(length);
        read(ret.array(), 0, length);
        return ret;
    }

    protected void reBuffer()
    {
        source.reBuffer();
    }

    @Override
    public byte readByte() throws IOException
    {
        byte b = source.readByte();
        crc.update(b);
        limit--;
        return b;
    }

    @Override
    public int read(byte[] buff, int offset, int length) throws IOException
    {
        if (length > limit)
            throw new IOException("Digest mismatch exception");

        int copied = source.read(buff, offset, length);
        crc.update(buff, offset, copied);
        limit -= copied;
        return copied;
    }
}
