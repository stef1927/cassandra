package org.apache.cassandra.utils;

import java.io.File;

import org.junit.Test;

import org.apache.cassandra.io.util.FileUtils;

public class CLibraryTest
{
    @Test
    public void testSkipCache()
    {
        File file = FileUtils.createTempFile("testSkipCache", "1");

        int fd = CLibrary.getfd(file.getPath());
        CLibrary.trySkipCache(fd, 0, 0);
    }
}
