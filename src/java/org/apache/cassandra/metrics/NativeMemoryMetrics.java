/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.metrics;

import com.codahale.metrics.Gauge;

import io.netty.util.internal.PlatformDependent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;

import org.apache.cassandra.utils.UnsafeMemoryAccess;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class NativeMemoryMetrics
{
    private static final Logger logger = LoggerFactory.getLogger(NativeMemoryMetrics.class);

    public static NativeMemoryMetrics instance = new NativeMemoryMetrics();
    public static long smallBufferThreshold = 4 * UnsafeMemoryAccess.pageSize();

    private final MetricNameFactory factory;
    private final BufferPoolMXBean directBufferPool;

    /** The total number of page-aligned direct byte buffer allocations that were done using
     * {@link org.apache.cassandra.io.compress.BufferType#OFF_HEAP_ALIGNED} */
    public final Meter totalAlignedAllocations;

    /** The number of page-aligned direct byte buffer allocations that are less than the small buffer threshold defined above,
     * currently 16k, and that were done using {@link org.apache.cassandra.io.compress.BufferType#OFF_HEAP_ALIGNED} */
    public final Meter smallAlignedAllocations;

    /** Total size of memory allocated directly by calling Native.malloc via {@link UnsafeMemoryAccess}, bypassing the JVM.
     * This is in addition to nio direct memory, for example off-heap memtables will use this type of memory. */
    public final Gauge<Long> rawNativeMemory;

    /** The memory allocated for direct byte buffers, aligned or otherwise, without counting any padding due to alignment.
     *  If {@code -Dio.netty.directMemory} is not set to {@code 0}, the direct memory used by Netty is <em>not</em> included in this value. */
    public final Gauge<Long> usedNioDirectMemory;

    /** The total memory allocated for direct byte buffers, aligned or otherwise, including any padding due to alignment.
     * If -Dsun.nio.PageAlignDirectMemory=true is not set then this will be identical to usedNioDirectMemory.
     * If {@code -Dio.netty.directMemory} is not set to {@code 0}, the direct memory used by Netty is <em>not</em> included in this value. */
    public final Gauge<Long> totalNioDirectMemory;

    /** The memory used by direct byte buffers allocated via the Netty library. These buffers are used for network communications.
     * A limit can be set with "-Dio.netty.maxDirectMemory". When this property is zero (the default in jvm.options), then
     * Netty will use the JVM NIO direct memory. Therefore, this value will be included in {@link #usedNioDirectMemory}
     * and {@link #totalNioDirectMemory} only when the property is set to zero, otherwise this value is extra. */
    public final Gauge<Long> networkDirectMemory;

    /** The total number of direct byte buffers allocated, aligned or otherwise. */
    public final Gauge<Long> nioDirectBufferCount;

    /** The total memory allocated, including direct byte buffers, network direct memory, and raw malloc memory */
    public final Gauge<Long> totalMemory;

    public NativeMemoryMetrics()
    {
        factory = new DefaultNameFactory("NativeMemory");
        directBufferPool = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)
                                            .stream()
                                            .filter(bpMBean -> bpMBean.getName().equals("direct"))
                                            .findFirst()
                                            .orElse(null);

        if (directBufferPool == null)
            logger.error("Direct memory buffer pool MBean not present, native memory metrics will be missing for nio buffers");

        totalAlignedAllocations = Metrics.meter(factory.createMetricName("TotalAlignedAllocations"));
        smallAlignedAllocations = Metrics.meter(factory.createMetricName("SmallAlignedAllocations"));

        rawNativeMemory = Metrics.register(factory.createMetricName("RawNativeMemory"), this::rawNativeMemory);
        usedNioDirectMemory = Metrics.register(factory.createMetricName("UsedNioDirectMemory"), this::usedNioDirectMemory);
        totalNioDirectMemory = Metrics.register(factory.createMetricName("TotalNioDirectMemory"), this::totalNioDirectMemory);
        networkDirectMemory = Metrics.register(factory.createMetricName("NetworkDirectMemory"), this::networkDirectMemory);
        nioDirectBufferCount = Metrics.register(factory.createMetricName("NioDirectBufferCount"), this::nioDirectBufferCount);
        totalMemory = Metrics.register(factory.createMetricName("TotalMemory"), this::totalMemory);
    }

    public long rawNativeMemory()
    {
        return UnsafeMemoryAccess.allocated();
    }

    public long usedNioDirectMemory()
    {
        return directBufferPool == null ? 0 : directBufferPool.getMemoryUsed();
    }

    public long totalNioDirectMemory()
    {
        return directBufferPool == null ? 0 : directBufferPool.getTotalCapacity();
    }

    public long nioDirectBufferCount()
    {
        return directBufferPool == null ? 0 : directBufferPool.getCount();
    }

    public long totalMemory()
    {
        // Use totalNioDirectMemory() instead of usedNioDirectMemory() because without
        // -Dsun.nio.PageAlignDirectMemory=true the two are identical. If someone adds
        // this flag again, we would prefer to include the JVM padding in the total memory.
        // Also only add the network memory if it's not allocated as NIO direct memory
        return rawNativeMemory() + totalNioDirectMemory() + (usingNioMemoryForNetwork() ? 0 : networkDirectMemory());
    }

    public long networkDirectMemory()
    {
        return PlatformDependent.usedDirectMemory();
    }

    public boolean usingNioMemoryForNetwork()
    {
        return !PlatformDependent.useDirectBufferNoCleaner();
    }
}
