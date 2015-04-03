package org.apache.cassandra.io.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest
{

    @Test
    public void testTruncate() throws IOException
    {
        File file = FileUtils.createTempFile("testTruncate", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        Files.write(file.toPath(), expected.getBytes());
        assertTrue(file.exists());

        byte[] b = Files.readAllBytes(file.toPath());
        assertEquals(expected, new String(b, Charset.forName("UTF-8")));

        FileUtils.truncate(file.getAbsolutePath(), 10);
        b = Files.readAllBytes(file.toPath());
        assertEquals("The quick ", new String(b, Charset.forName("UTF-8")));

        FileUtils.truncate(file.getAbsolutePath(), 0);
        b = Files.readAllBytes(file.toPath());
        assertEquals(0, b.length);
    }

}
