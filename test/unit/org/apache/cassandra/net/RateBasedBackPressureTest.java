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

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.utils.TestTimeSource;

import static org.apache.cassandra.net.RateBasedBackPressure.FACTOR;
import static org.apache.cassandra.net.RateBasedBackPressure.HIGH_RATIO;
import static org.apache.cassandra.net.RateBasedBackPressure.LOW_RATIO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0", LOW_RATIO, "1", FACTOR, "2"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeSmallerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "2", LOW_RATIO, "1", FACTOR, "2"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLowRatioMustBeBiggerThanZero() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0", FACTOR, "2"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testLowRatioMustBeSmallerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "1.1", FACTOR, "2"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testHighRatioMustBeBiggerThanLowRatio() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.8", LOW_RATIO, "0.9", FACTOR, "2"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testFactorMustBeBiggerEqualThanOne() throws Exception
    {
        new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "0"));
    }
    
    @Test
    public void testBackPressureIsNotUpdatedBeyondInfinity() throws Exception
    {
        long windowSize = 5000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
        
        // Get initial rate:
        double initialRate = state.outgoingLimiter.getRate();
        assertEquals(Double.POSITIVE_INFINITY, initialRate, 0.0);
        
        // Update incoming and outgoing rate equally:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);
        
        // Apply and verify the rate doesn't change because already at infinity:
        strategy.apply(state);
        assertEquals(initialRate, state.outgoingLimiter.getRate(), 0.0);
    }
    
    @Test
    public void testBackPressureIsUpdatedOncePerWindowSize() throws Exception
    {
        long windowSize = 5000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
        
        // Get initial time:
        long current = state.getLastAcquire();
        assertEquals(0, current);
        
        // Update incoming and outgoing rate:
        state.incomingRate.update(1);
        state.outgoingRate.update(1);
        
        // Apply and verify the timestamp changed:
        strategy.apply(state);
        current = state.getLastAcquire();
        assertEquals(timeSource.currentTimeMillis(), current);
        
        // Move time ahead:
        long previous = current;
        timeSource.sleep(1, TimeUnit.SECONDS);
        
        // Apply and verify the last check didn't change because below the window size:
        strategy.apply(state);
        current = state.getLastAcquire();
        assertEquals(previous, current);
    }
    
    @Test
    public void testBackPressureIsNotBiasedByRecentUpdates() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
        
        // Get initial rate:
        double initialRate = state.outgoingLimiter.getRate();
        assertEquals(Double.POSITIVE_INFINITY, initialRate, 0.0);
        
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Apply and verify the rate didn't change because the updates were too recent (newer than half the window sise):
        strategy.apply(state);
        assertEquals(initialRate, state.outgoingLimiter.getRate(), 0.0);
    }
    
    @Test
    public void testBackPressureWhenBelowHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
        
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Apply and verify the rate is decreased by factor:
        strategy.apply(state);
        assertEquals(15.0, state.outgoingLimiter.getRate(), 0.1);
    }
    
    @Test
    public void testBackPressureWhenBelowLowRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
         
        // Update incoming and outgoing rate so that the ratio is 0.01:
        state.incomingRate.update(1);
        state.outgoingRate.update(100);
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Apply and verify the strategy sets overload=true:
        strategy.apply(state);
        assertTrue(state.overload.get());
    }
    
    @Test
    public void testBackPressureOverloadIsResetAfterWindowSize() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
             
        // Update incoming and outgoing rate so that the ratio is 0.01:
        state.incomingRate.update(1);
        state.outgoingRate.update(100);
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Apply and verify the strategy sets overload=true:
        strategy.apply(state);
        assertTrue(state.overload.get());
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify the overload state is reset and the rate limiter changed to the min value:
        strategy.apply(state);
        assertFalse(state.overload.get());
        assertEquals(10.0, state.outgoingLimiter.getRate(), 0.0);
    }
    
    @Test
    public void testBackPressureRateLimiterIsIncreasedAfterGoingAboveHighRatio() throws Exception
    {
        long windowSize = 6000;
        TestTimeSource timeSource = new TestTimeSource();
        BackPressureState state = new BackPressureState(timeSource, windowSize);
        RateBasedBackPressure strategy = new RateBasedBackPressure(ImmutableMap.of(HIGH_RATIO, "0.9", LOW_RATIO, "0.1", FACTOR, "10"));
                
        // Update incoming and outgoing rate so that the ratio is 0.5:
        state.incomingRate.update(50);
        state.outgoingRate.update(100);
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Apply and verify the rate changed:
        strategy.apply(state);
        assertEquals(15.0, state.outgoingLimiter.getRate(), 0.1);
        
        // Update incoming and outgoing rate back above high rate:
        state.incomingRate.update(50);
        state.outgoingRate.update(50);
        
        // Move time ahead of window size:
        timeSource.sleep(windowSize, TimeUnit.MILLISECONDS);
        
        // Verify rate limiter is increased by factor:
        strategy.apply(state);
        assertFalse(state.overload.get());
        assertEquals(16.5, state.outgoingLimiter.getRate(), 0.1);
    }
}
