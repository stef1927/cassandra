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
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.utils.SlidingTimeRate;
import org.apache.cassandra.utils.TimeSource;

/**
 * The rate-based back-pressure state, tracked per replica host.
 * <br/><br/>
 *
 * This back-pressure state is made up of the following attributes:
 * <ul>
 * <li>windowSize: the length of the back-pressure window in milliseconds.</li>
 * <li>incomingRate: the rate of back-pressure supporting incoming messages.</li>
 * <li>outgoingRate: the rate of back-pressure supporting outgoing messages.</li>
 * <li>overload: a boolean flag to notify the destination replica host is overloaded.</li>
 * <li>outgoingLimiter: the rate limiter to eventually apply to outgoing messages.</li>
 * </ul>
 *
 * It also provides methods to exclusively acquire/release back-pressure windows at given intervals; 
 * this allows to apply back-pressure even under concurrent modifications. Please also note a write lock is acquired
 * during window acquisition so that no concurrent rate updates can screw rate computations.
 */
public class RateBasedBackPressureState implements BackPressureState
{
    private final InetAddress host;
    private final AtomicLong lastAcquire;
    private final ReentrantReadWriteLock acquireLock;
    private final TimeSource timeSource;
    final long windowSize;
    final SlidingTimeRate incomingRate;
    final SlidingTimeRate outgoingRate;
    final RateLimiter outgoingLimiter;

    RateBasedBackPressureState(InetAddress host, TimeSource timeSource, long windowSize)
    {
        this.host = host;
        this.timeSource = timeSource;
        this.windowSize = windowSize;
        this.lastAcquire = new AtomicLong();
        this.acquireLock = new ReentrantReadWriteLock();
        this.incomingRate = new SlidingTimeRate(timeSource, this.windowSize, this.windowSize / 10, TimeUnit.MILLISECONDS);
        this.outgoingRate = new SlidingTimeRate(timeSource, this.windowSize, this.windowSize / 10, TimeUnit.MILLISECONDS);
        this.outgoingLimiter = RateLimiter.create(Double.POSITIVE_INFINITY);
    }
    
    @VisibleForTesting
    public long getLastAcquire()
    {
        return lastAcquire.get();
    }

    @Override
    public void onMessageSent()
    {
        acquireLock.readLock().lock();
        try
        {
            outgoingRate.update(1);
        }
        finally
        {
            acquireLock.readLock().unlock();
        }
    }

    @Override
    public void onResponseReceived()
    {
        acquireLock.readLock().lock();
        try
        {
            incomingRate.update(1);
        }
        finally
        {
            acquireLock.readLock().unlock();
        }
    }

    @Override
    public double getBackPressureRateLimit()
    {
        return outgoingLimiter.getRate();
    }

    @Override
    public InetAddress getHost()
    {
        return host;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof RateBasedBackPressureState)
        {
            RateBasedBackPressureState other = (RateBasedBackPressureState) obj;
            return this.host.equals(other.host);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.host.hashCode();
    }
    
    @VisibleForTesting
    public void doRateLimit() 
    {
        outgoingLimiter.acquire(1);
    }

    boolean tryAcquireNewWindow(long interval)
    {
        long now = timeSource.currentTimeMillis();
        boolean acquired = (now - lastAcquire.get() >= interval) && acquireLock.writeLock().tryLock();
        if (acquired)
            lastAcquire.set(now);

        return acquired;
    }

    void releaseWindow()
    {
        acquireLock.writeLock().unlock();
    }
}
