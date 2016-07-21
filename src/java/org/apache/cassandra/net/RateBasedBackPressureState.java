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
 * <li>outgoingLimiter: the rate limiter to eventually apply to outgoing messages.</li>
 * </ul>
 * <br/>
 * The incomingRate and outgoingRate are updated together when a response is received to guarantee consistency between 
 * the two.
 * <br/>
 * It also provides methods to exclusively acquire/release back-pressure windows at given intervals; 
 * this allows to apply back-pressure even under concurrent modifications. Please also note a write lock is acquired
 * during window acquisition so that no concurrent rate updates can screw rate computations.
 */
class RateBasedBackPressureState implements BackPressureState
{
    private final InetAddress host;
    private final AtomicLong lastAcquire;
    private final ReentrantReadWriteLock acquireLock;
    private final TimeSource timeSource;
    private final long windowSize;
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

    @Override
    public void onMessageSent(MessageOut<?> message) {}

    @Override
    public void onResponseReceived()
    {
        acquireLock.readLock().lock();
        try
        {
            incomingRate.update(1);
            outgoingRate.update(1);
        }
        finally
        {
            acquireLock.readLock().unlock();
        }
    }

    @Override
    public void onResponseTimeout()
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

    @Override
    public String toString()
    {
        return String.format("[host: %s, incoming rate: %.3f, outgoing rate: %.3f, rate limit: %.3f]",
                             host, incomingRate.get(TimeUnit.SECONDS), outgoingRate.get(TimeUnit.SECONDS), outgoingLimiter.getRate());
    }

    @VisibleForTesting
    void doRateLimit() 
    {
        outgoingLimiter.acquire(1);
    }

    @VisibleForTesting
    long getLastAcquire()
    {
        return lastAcquire.get();
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
