/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.net;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.utils.TestTimeSource;
import org.apache.cassandra.utils.TimeSource;

import static org.apache.cassandra.net.RateBasedBackPressure.FACTOR;
import static org.apache.cassandra.net.RateBasedBackPressure.FLOW;
import static org.apache.cassandra.net.RateBasedBackPressure.HIGH_RATIO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RateBasedBackPressureTest
{
    @Test(expected = IllegalArgumentException.class)
    public void testAcceptsNoLessThanThreeArguments() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "1"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeBiggerThanZero() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0", FACTOR, "2", FLOW, "FAST"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeSmallerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "2", FACTOR, "2", FLOW, "FAST"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testFactorMustBeBiggerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "0", FLOW, "FAST"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testWindowSizeMustBeBiggerEqualThanTen() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "5", FLOW, "FAST"), new TestTimeSource(), 1);
    }
    
    @Test
    public void testFlowMustBeEitherFASTorSLOW() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "FAST"));
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "SLOW"));
        try
        {
            new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "1", FLOW, "WRONG"));
            fail("Expected to fail with wrong flow type.");
        }
        catch (Exception ex)
        {
        }
    }

    @Test
    public void testBackPressureStateUpdates()
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);

        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onMessageSent(null);
        assertEquals(0, state.incomingRate.size());
        assertEquals(0, state.outgoingRate.size());

        state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onResponseReceived();
        assertEquals(1, state.incomingRate.size());
        assertEquals(1, state.outgoingRate.size());

        state = strategy.newState(InetAddress.getLoopbackAddress());
        state.onResponseTimeout();
        assertEquals(0, state.incomingRate.size());
        assertEquals(1, state.outgoingRate.size());
    }
    
    @Test
    public void testBackPressureIsNotUpdatedBeyondInfinity() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        
        // Get initial rate:
        double initialRate = state.outgoingLimiter.getRate();
        assertEquals(Double.POSITIVE_INFINITY, initialRate, 0.0);
        
        // Update incoming and outgoing rate equally:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the rate doesn't change because already at infinity:
        strategy.apply(Arrays.asList(state));
        assertEquals(initialRate, state.outgoingLimiter.getRate(), 0.0);
    }
    
    @Test
    public void testBackPressureIsUpdatedOncePerWindowSize() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        
        // Get initial time:
        long current = state.getLastAcquire();
        assertEquals(0, current);
        
        // Update incoming and outgoing rate:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);
        
        // Move time ahead by window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the timestamp changed:
        strategy.apply(Arrays.asList(state));
        current = state.getLastAcquire();
        assertEquals(timeSource.currentTimeMillis(), current);
        
        // Move time ahead by less than interval:
        long previous = current;
        timeSource.sleep(windowSize / 2, TimeUnit.MILLISECONDS);
        
        // Verify the last timestamp didn't change because below the window size:
        strategy.apply(Arrays.asList(state));
        current = state.getLastAcquire();
        assertEquals(previous, current);
    }
    
    @Test
    public void testBackPressureWhenBelowHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the rate is decreased by incoming/outgoing:
        strategy.apply(Arrays.asList(state));
        assertEquals(7.4, state.outgoingLimiter.getRate(), 0.1);
    }
    
    @Test
    public void testBackPressureRateLimiterIsIncreasedAfterGoingAgainAboveHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
                
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the rate decreased:
        strategy.apply(Arrays.asList(state));
        assertEquals(7.4, state.outgoingLimiter.getRate(), 0.1);
        
        // Update incoming and outgoing rate back above high rate:
        state.incomingRate.update(50);
        state.outgoingRate.update(50);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify rate limiter is increased by factor:
        strategy.apply(Arrays.asList(state));
        assertEquals(8.25, state.outgoingLimiter.getRate(), 0.1);
        
        // Update incoming and outgoing rate to keep it below the limiter rate:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify rate limiter is not increased as already higher than the actual rate:
        strategy.apply(Arrays.asList(state));
        assertEquals(8.25, state.outgoingLimiter.getRate(), 0.1);
    }
    
    @Test
    public void testBackPressureFastFlow() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        TestableBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        TestableBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        TestableBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(50);
        state1.outgoingRate.update(100);
        state2.incomingRate.update(100); // fast
        state2.outgoingRate.update(100);
        state3.incomingRate.update(20);
        state3.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the fast replica has been selected:
        List<RateBasedBackPressureState> input = Arrays.asList(state1, state2, state3);
        strategy.apply(input);
        assertFalse(state1.checkApplied());
        assertTrue(state2.checkApplied());
        assertFalse(state3.checkApplied());
    }

    @Test
    public void testBackPressureSlowFlow() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        TestableBackPressure strategy = new TestableBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "SLOW"), timeSource, windowSize);
        TestableBackPressureState state1 = strategy.newState(InetAddress.getByName("127.0.0.1"));
        TestableBackPressureState state2 = strategy.newState(InetAddress.getByName("127.0.0.2"));
        TestableBackPressureState state3 = strategy.newState(InetAddress.getByName("127.0.0.3"));

        // Update incoming and outgoing rates:
        state1.incomingRate.update(50);
        state1.outgoingRate.update(100);
        state2.incomingRate.update(100);
        state2.outgoingRate.update(100);
        state3.incomingRate.update(20); // slow
        state3.outgoingRate.update(100);

        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);

        // Verify the slow replica has been selected:
        List<RateBasedBackPressureState> input = Arrays.asList(state1, state2, state3);
        strategy.apply(input);
        assertFalse(state1.checkApplied());
        assertFalse(state2.checkApplied());
        assertTrue(state3.checkApplied());
    }
    
    @Test
    public void testBackPressureIsResetPastWindowSize() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", FACTOR, "10", FLOW, "FAST"), timeSource, windowSize);
        RateBasedBackPressureState state = strategy.newState(InetAddress.getLoopbackAddress());
        
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Move time ahead:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the rate is decreased by incoming/outgoing:
        strategy.apply(Arrays.asList(state));
        assertEquals(7.4, state.outgoingLimiter.getRate(), 0.1);
        
        // Move time ahead more than window size:
        timeSource.sleep(windowSize + 1, TimeUnit.MILLISECONDS);
        
        // Verify the rate is reset:
        strategy.apply(Arrays.asList(state));
        assertEquals(Double.POSITIVE_INFINITY, state.outgoingLimiter.getRate(), 0.0);
    }
    
    public static class TestableBackPressure extends RateBasedBackPressure
    {
        public TestableBackPressure(Map<String, Object> args, TimeSource timeSource, long windowSize)
        {
            super(args, timeSource, windowSize);
        }

        @Override
        public TestableBackPressureState newState(InetAddress host)
        {
            return new TestableBackPressureState(host, this.timeSource, this.windowSize);
        }
    }

    public static class TestableBackPressureState extends RateBasedBackPressureState
    {
        public volatile boolean applied = false;

        public TestableBackPressureState(InetAddress host, TimeSource timeSource, long windowSize)
        {
            super(host, timeSource, windowSize);
        }

        @Override
        public void doRateLimit()
        {
            super.doRateLimit();
            applied = true;
        }

        public boolean checkApplied()
        {
            boolean checked = applied;
            applied = false;
            return checked;
        }
    }
}
