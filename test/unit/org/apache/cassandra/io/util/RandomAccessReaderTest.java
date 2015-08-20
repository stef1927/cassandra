package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.io.compress.BufferType;

public class RandomAccessReaderTest
{
    private static final class Parameters
    {
        public final long fileLength;
        public final int bufferSize;

        public BufferType bufferType;
        public int maxSegmentSize;
        public boolean mmappedRegions;

        public Parameters(long fileLength, int bufferSize)
        {
            this.fileLength = fileLength;
            this.bufferSize = bufferSize;
            this.bufferType = BufferType.OFF_HEAP;
            this.maxSegmentSize = MmappedRegions.MAX_SEGMENT_SIZE;
            this.mmappedRegions = false;
        }

        public Parameters mmappedRegions(boolean mmappedRegions)
        {
            this.mmappedRegions = mmappedRegions;
            return this;
        }

        public Parameters bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        public Parameters maxSegmentSize(int maxSegmentSize)
        {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }
    }

    @Test
    public void testBufferedOffHeap() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).bufferType(BufferType.OFF_HEAP));
    }

    @Test
    public void testBufferedOnHeap() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testBigBufferSize() throws IOException
    {
        testReadFully(new Parameters(8192, 65536).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testTinyBufferSize() throws IOException
    {
        testReadFully(new Parameters(8192, 16).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testOneSegment() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).mmappedRegions(true));
    }

    @Test
    public void testMultipleSegments() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).mmappedRegions(true).maxSegmentSize(1024));
    }

    private void testReadFully(Parameters params) throws IOException
    {
        final File f = File.createTempFile("testReadFully", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        SequentialWriter writer = SequentialWriter.open(f);
        int numWritten = 0;
        while (numWritten < params.fileLength)
        {
            writer.write(expected.getBytes());
            numWritten += expected.length();
        }

        writer.finish();
        assert f.exists();
        assert f.length() >= params.fileLength;

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader.Builder builder = new RandomAccessReader.Builder(channel)
                                             .bufferType(params.bufferType)
                                             .bufferSize(params.bufferSize);
        if (params.mmappedRegions)
            builder.regions(MmappedRegions.empty(channel).extend(f.length()));

        RandomAccessReader reader = builder.build();
        assertEquals(f.getAbsolutePath(), reader.getPath());
        assertEquals(f.length(), reader.length());

        byte[] b = new byte[expected.length()];
        long numRead = 0;
        while (numRead < params.fileLength)
        {
            reader.readFully(b);
            assertEquals(expected, new String(b));
            numRead += b.length;
        }

        assertTrue(reader.isEOF());
        assertEquals(0, reader.bytesRemaining());

        reader.close();
        reader.close(); // should be idem-potent

        assertNull(builder.regions.close(null));
        channel.close();
    }

    @Test
    public void testReadBytes() throws IOException
    {
        File f = File.createTempFile("testReadBytes", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        SequentialWriter writer = SequentialWriter.open(f);
        writer.write(expected.getBytes());
        writer.finish();

        assert f.exists();

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader reader = new RandomAccessReader.Builder(channel).build();
        assertEquals(f.getAbsolutePath(), reader.getPath());
        assertEquals(expected.length(), reader.length());

        ByteBuffer b = reader.readBytes(expected.length());
        assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));

        assertTrue(reader.isEOF());
        assertEquals(0, reader.bytesRemaining());

        reader.close();
        channel.close();
    }

    @Test
    public void testReset() throws IOException
    {
        File f = File.createTempFile("testMark", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";
        final int numIterations = 10;

        SequentialWriter writer = SequentialWriter.open(f);
        for (int i = 0; i < numIterations; i++)
            writer.write(expected.getBytes());
        writer.finish();

        assert f.exists();

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader reader = new RandomAccessReader.Builder(channel).build();
        assertEquals(expected.length() * numIterations, reader.length());

        ByteBuffer b = reader.readBytes(expected.length());
        assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));

        assertFalse(reader.isEOF());
        assertEquals((numIterations - 1) * expected.length(), reader.bytesRemaining());

        FileMark mark = reader.mark();
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));

        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }
        assertTrue(reader.isEOF());
        assertEquals(expected.length() * (numIterations -1), reader.bytesPastMark());
        assertEquals(expected.length() * (numIterations - 1), reader.bytesPastMark(mark));

        reader.reset(mark);
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));
        assertFalse(reader.isEOF());
        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }

        reader.reset();
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));
        assertFalse(reader.isEOF());
        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }

        assertTrue(reader.isEOF());
        reader.close();
        channel.close();
    }

    @Test
    public void testSeekSingleThread() throws IOException, InterruptedException
    {
        testSeek(1);
    }

    @Test
    public void testSeekMultipleThreads() throws IOException, InterruptedException
    {
        testSeek(10);
    }

    private void testSeek(int numThreads) throws IOException, InterruptedException
    {
        final File f = File.createTempFile("testMark", "1");
        final byte[] expected = new byte[1 << 16];

        long seed = System.nanoTime();
        //seed = 365238103404423L;
        System.out.println("Seed " + seed);
        Random r = new Random(seed);
        r.nextBytes(expected);

        SequentialWriter writer = SequentialWriter.open(f);
        writer.write(expected);
        writer.finish();

        assert f.exists();

        final ChannelProxy channel = new ChannelProxy(f);

        final Runnable worker = () ->
        {
            try
            {
                RandomAccessReader reader = new RandomAccessReader.Builder(channel).build();
                assertEquals(expected.length, reader.length());

                ByteBuffer b = reader.readBytes(expected.length);
                assertTrue(Arrays.equals(expected, b.array()));
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());

                reader.seek(0);
                b = reader.readBytes(expected.length);
                assertTrue(Arrays.equals(expected, b.array()));
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());

                for (int i = 0; i < 10; i++)
                {
                    int pos = r.nextInt(expected.length);
                    reader.seek(pos);
                    assertEquals(pos, reader.getPosition());

                    ByteBuffer buf = ByteBuffer.wrap(expected, pos, expected.length - pos)
                                               .order(ByteOrder.BIG_ENDIAN);

                    while(reader.bytesRemaining() > 4)
                        assertEquals(buf.getInt(), reader.readInt());

                }

                reader.close();
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        };

        if(numThreads == 1)
        {
            worker.run();
        }
        else
        {
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            for (int i = 0; i < numThreads; i++)
                executor.submit(worker);

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }

        channel.close();
    }
}
