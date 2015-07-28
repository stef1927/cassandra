package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

public class RandomAccessReaderTest
{
    private static final class Parameters
    {
        public final long fileLength;
        public final int bufferSize;

        public List<Long> boundaries;
        public BufferType bufferType;
        public int maxSegmentSize;

        public Parameters(long fileLength, int bufferSize)
        {
            this.fileLength = fileLength;
            this.bufferSize = bufferSize;
            this.boundaries = new ArrayList<>();
            this.bufferType = BufferType.OFF_HEAP;
            this.maxSegmentSize = Integer.MAX_VALUE;
        }

        public Parameters boundaries(List<Long> boundaries)
        {
            this.boundaries.addAll(boundaries);
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
        testReadFully(new Parameters(8192, 4096).boundaries(Arrays.asList(0L)));
    }

    @Test
    public void testMultipleSegments() throws IOException
    {
        testReadFully(new Parameters(4096, 4096).boundaries(Arrays.asList(0L, 1024L, 2048L, 3072L)));
    }

    @Test
    public void testMissingInitialSegment() throws IOException
    {
        testReadFully(new Parameters(4096, 4096).boundaries(Arrays.asList(1024L, 2048L, 3072L)).maxSegmentSize(1024));
    }

    @Test
    public void testMissingMiddleSegments() throws IOException
    {
        testReadFully(new Parameters(5120, 4096).boundaries(Arrays.asList(0L, 2048L, 4096L)).maxSegmentSize(1024));
    }

    @Test
    public void testMissingFinalSegment() throws IOException
    {
        testReadFully(new Parameters(4096, 4096).boundaries(Arrays.asList(0L, 1024L, 2048L)).maxSegmentSize(1024));
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
        Map<Long, ByteBuffer> segments = new TreeMap<>();
        for (int i = 0; i < params.boundaries.size(); i++)
        {
            long start = params.boundaries.get(i);
            long end = (i == params.boundaries.size() - 1 ? params.fileLength : params.boundaries.get(i + 1));
            long realend = (i == params.boundaries.size() - 1 ? f.length() : params.boundaries.get(i + 1));

            if ((end - start) <= params.maxSegmentSize)
                segments.put(start, channel.map(FileChannel.MapMode.READ_ONLY, start, realend - start));
        }

        RandomAccessReader reader = new RandomAccessReader.Builder(channel)
                                    .segments(segments)
                                    .bufferType(params.bufferType)
                                    .bufferSize(params.bufferSize)
                                    .build();
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

        segments.values().forEach(FileUtils::clean);
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
        final String[] expected = new String[10];
        int len = 0;
        for (int i = 0; i < expected.length; i++)
        {
            expected[i] = UUID.randomUUID().toString();
            len += expected[i].length();
        }
        final int totalLength = len;

        SequentialWriter writer = SequentialWriter.open(f);
        for (int i = 0; i < expected.length; i++)
            writer.write(expected[i].getBytes());
        writer.finish();

        assert f.exists();

        final ChannelProxy channel = new ChannelProxy(f);

        final Runnable worker = () ->
        {
            try
            {
                RandomAccessReader reader = new RandomAccessReader.Builder(channel).build();
                assertEquals(totalLength, reader.length());

                ByteBuffer b = reader.readBytes(expected[0].length());
                assertEquals(expected[0], new String(b.array(), Charset.forName("UTF-8")));

                assertFalse(reader.isEOF());
                assertEquals(totalLength - expected[0].length(), reader.bytesRemaining());

                long filePointer = reader.getFilePointer();

                for (int i = 1; i < expected.length; i++)
                {
                    b = reader.readBytes(expected[i].length());
                    assertEquals(expected[i], new String(b.array(), Charset.forName("UTF-8")));
                }
                assertTrue(reader.isEOF());

                reader.seek(filePointer);
                assertFalse(reader.isEOF());
                for (int i = 1; i < expected.length; i++)
                {
                    b = reader.readBytes(expected[i].length());
                    assertEquals(expected[i], new String(b.array(), Charset.forName("UTF-8")));
                }

                assertTrue(reader.isEOF());
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
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
            executor.submit(worker);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        channel.close();
    }
}
