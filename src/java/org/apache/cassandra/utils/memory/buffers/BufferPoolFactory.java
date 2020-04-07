/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package org.apache.cassandra.utils.memory.buffers;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.BufferPoolMetrics;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.PageAware;

import static org.apache.cassandra.utils.memory.buffers.BufferPool.*;

/**
 * Holds the global buffer pool instances and can be used to create a new buffer pool for benchmarks or testing.
 * <p/>
 * Two instances currently exist:
 * - the buffer pool for cached reads, to be used exclusively by the {@link org.apache.cassandra.cache.ChunkCache}.
 * - the buffer pool for direct reads, to be used by read operations that require a temporary byte buffer. These buffers
 *   must be returned to the pool quickly afterwards, they must not be retained for a long period.
 */
public class BufferPoolFactory
{
    /**
     * The buffer pool instance used by the chunk cache. It's created with a maximum pool size of
     * {@link DatabaseDescriptor#getFileCacheSizeInMB()} Mib.
     */
    private final static BufferPool fileCache = createPermanent("CachedReadsBufferPool",
                                                                BufferPoolMXBean.CACHED_FILE_READS_MBEAN_NAME,
                                                                PageAware.PAGE_SIZE, // file cache only uses PAGE_SIZE buffer sizes
                                                                PageAware.PAGE_SIZE,
                                                                DatabaseDescriptor.getFileCacheSizeInMB() * 1024L * 1024L);

    /**
     * The buffer pool instance used for direct reads. It's created with a maximum pool size of
     * {@link DatabaseDescriptor#getDirectReadsCacheSizeInMB()} and it can be used for reading hints, streaming checksummed
     * files or by the chunk readers when the cache is disabled.
     */
    private final static BufferPool directReads = createTemporary("DirectReadsBufferPool",
                                                                  BufferPoolMXBean.DIRECT_FILE_READS_MBEAN_NAME,
                                                                  THREAD_LOCAL_SLAB_SIZE,
                                                                  MAX_DIRECT_READS_BUFFER_SIZE * 2,
                                                                  DatabaseDescriptor.getDirectReadsCacheSizeInMB() * 1024L * 1024L);

    /**
     * The legacy name of the buffer pool metrics was "BufferPool" before the pool was divided, so we create these aliases
     * to make sure the old metrics are still available.
     */
    static
    {
        BufferPoolMetrics.registerLegacyAliases("BufferPool", fileCache.metrics(), directReads.metrics());
    }

    public static BufferPool forCachedReads()
    {
        return BufferPoolFactory.fileCache;
    }

    public static BufferPool forDirectReads()
    {
        return BufferPoolFactory.directReads;
    }

    /**
     * Create a buffer pool suitable for potentially long-lived buffers, such as those stored in a cache.
     *
     * @param name the name of the pool, for the metrics
     * @param mbeanName the mbean name, for nodetool bufferpool status
     * @param minBufferSize the minimum buffer size, below this buffer sizes are rounded up
     * @param maxBufferSize the maximum buffer size, beyond this direct allocations are performed
     * @param maxPoolSize the maximum size of the pool, memory below this value is not released but kept for later
     *
     * @return the buffer pool just created
     */
    public static BufferPool createPermanent(String name, String mbeanName, int minBufferSize, int maxBufferSize, long maxPoolSize)
    {
        BufferPool ret = BufferPool.DISABLED ? new BufferPoolDisabled(name, maxPoolSize) : new PermanentBufferPool(name, minBufferSize, maxBufferSize, maxPoolSize);
        if (mbeanName != null)
            register(mbeanName, ret);

        return ret;
    }

    /**
     * Create a buffer pool suitable for temporary buffers, that must be released quickly.
     *
     * @param name the name of the pool, for the metrics
     * @param mbeanName the mbean name, for nodetool bufferpool status
     * @param slabSize the size of the slab for slicing temporary buffers from
     * @param maxBufferSize the maximum buffer size, beyond this direct allocations are performed
     * @param maxPoolSize the maximum size of the pool, memory below this value is not released but kept for later
     *
     * @return the buffer pool just created
     */
    public static BufferPool createTemporary(String name, String mbeanName, int slabSize, int maxBufferSize, long maxPoolSize)
    {
        BufferPool ret = BufferPool.DISABLED ? new BufferPoolDisabled(name, maxPoolSize) : new TemporaryBufferPool(name, slabSize, maxBufferSize, maxPoolSize);
        if (mbeanName != null)
            register(mbeanName, ret);

        return ret;
    }

    /**
     * Returns a string status summary of the two buffer pools currently managed by this factory (for direct reads and for
     * cached reads), for logging purposes.
     */
    public static String getStatusSummary()
    {
        return String.format("Buffer pool size for cached reads: %s, for direct reads: %s%n",
                             FBUtilities.prettyPrintMemory(fileCache.allocatedMemoryBytes()),
                             FBUtilities.prettyPrintMemory(directReads.allocatedMemoryBytes())) +
               String.format("Cached reads buffer pool %s%n", fileCache.toString()) +
               String.format("Direct reads buffer pool %s%n", directReads.toString());
    }

    private static void register(String name, BufferPool pool)
    {
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

            ObjectName me = new ObjectName(name);
            if (!mbs.isRegistered(me))
                mbs.registerMBean(pool, new ObjectName(name));
        }
        catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException | MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
    }
}
