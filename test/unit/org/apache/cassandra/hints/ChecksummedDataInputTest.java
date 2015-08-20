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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.FBUtilities;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class ChecksummedDataInputTest
{
    @Test
    public void testThatItWorks() throws IOException
    {
        ByteBuffer buffer;
        int length;
        // fill a bytebuffer with some input
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.write(127);
            out.write(new byte[]{ 0, 1, 2, 3, 4, 5, 6 });
            out.writeBoolean(false);
            out.writeByte(10);
            out.writeChar('t');
            out.writeDouble(3.3);
            out.writeFloat(2.2f);
            out.writeInt(42);
            out.writeLong(Long.MAX_VALUE);
            out.writeShort(Short.MIN_VALUE);
            out.writeUTF("utf");

            length = out.getLength();
            buffer = out.buffer();
        }

        // save the buffer to file to create a RAR
        File file = File.createTempFile("testThatItWorks", "1");
        file.deleteOnExit();
        try (SequentialWriter writer = SequentialWriter.open(file))
        {
            writer.write(buffer);
            writer.finish();
        }

        assertTrue(file.exists());
        assertEquals(length, file.length());

        // calculate resulting CRC
        CRC32 crc = new CRC32();
        FBUtilities.updateChecksum(crc, buffer);
        int expectedCRC = (int) crc.getValue();

        try (RandomAccessReader rar = RandomAccessReader.open(file);
             ChecksummedDataInput crcInput = ChecksummedDataInput.wrap(rar))
        {
            crcInput.limit(buffer.remaining());

            // assert that we read all the right values back
            assertEquals(127, crcInput.read());
            byte[] bytes = new byte[7];
            crcInput.readFully(bytes);
            assertTrue(Arrays.equals(new byte[]{ 0, 1, 2, 3, 4, 5, 6 }, bytes));
            assertEquals(false, crcInput.readBoolean());
            assertEquals(10, crcInput.readByte());
            assertEquals('t', crcInput.readChar());
            assertEquals(3.3, crcInput.readDouble());
            assertEquals(2.2f, crcInput.readFloat());
            assertEquals(42, crcInput.readInt());
            assertEquals(Long.MAX_VALUE, crcInput.readLong());
            assertEquals(Short.MIN_VALUE, crcInput.readShort());
            assertEquals("utf", crcInput.readUTF());

            // assert that the crc matches, and that we've read exactly as many bytes as expected
            assertEquals(0, crcInput.bytesRemaining());
            assertEquals(expectedCRC, crcInput.getCrc());
        }
    }
}
