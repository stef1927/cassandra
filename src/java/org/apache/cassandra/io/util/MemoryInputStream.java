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

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.google.common.primitives.Ints;

import org.apache.cassandra.utils.memory.MemoryUtil;

public class MemoryInputStream extends RebufferingInputStream implements DataInput
{
    private final Memory mem;
    private long offset;

    public MemoryInputStream(Memory mem)
    {
        super(getByteBuffer(mem, 0));
        this.mem = mem;
        this.offset = 0;
    }

    @Override
    protected void reBuffer() throws IOException
    {
        offset += buffer.capacity();
        if ((mem.peer + offset) >= mem.size())
            return;

        buffer = getByteBuffer(mem, offset);
    }

    private static ByteBuffer getByteBuffer(Memory mem, long offset)
    {
        return MemoryUtil.getByteBuffer(mem.peer + offset, Ints.checkedCast(mem.size - offset)).order(ByteOrder.BIG_ENDIAN);
    }

    @Override
    public int available() throws IOException
    {
        return buffer.remaining();
    }
}
