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
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.utils.SystemTimeSource;
import org.apache.cassandra.utils.TimeSource;

/**
 * Back-pressure algorithm based on rate limiting according to the ratio between incoming and outgoing rates, computed
 * over a sliding time window with size equal to write RPC timeout.
 */
public class RateBasedBackPressure implements BackPressureStrategy<RateBasedBackPressureState>
{
    public static final String HIGH_RATIO = "high_ratio";
    public static final String FACTOR = "factor";
    public static final String FLOW = "flow";
    private static final String BACK_PRESSURE_HIGH_RATIO = "0.90";
    private static final String BACK_PRESSURE_FACTOR = "5";
    public static final String BACK_PRESSURE_FLOW = "FAST";

    private static final Logger logger = LoggerFactory.getLogger(RateBasedBackPressure.class);
    
    protected final TimeSource timeSource;
    protected final double highRatio;
    protected final int factor;
    protected final Flow flow;
    protected final long windowSize;
    
    public static enum Flow
    {
        FAST
        {
            @Override
            public RateBasedBackPressureState get(SortedSet<RateBasedBackPressureState> states)
            {
                return states.first();
            }
        },
        SLOW
        {
            @Override
            public RateBasedBackPressureState get(SortedSet<RateBasedBackPressureState> states)
            {
                return states.last();
            }
        };

        public abstract RateBasedBackPressureState get(SortedSet<RateBasedBackPressureState> states);
    }

    public static ParameterizedClass withDefaultParams()
    {
        return new ParameterizedClass(RateBasedBackPressure.class.getName(),
                                      ImmutableMap.of(HIGH_RATIO, BACK_PRESSURE_HIGH_RATIO,
                                                      FACTOR, BACK_PRESSURE_FACTOR,
                                                      FLOW, BACK_PRESSURE_FLOW));
    }

    public RateBasedBackPressure(Map<String, Object> args)
    {
        this(args, new SystemTimeSource(), DatabaseDescriptor.getWriteRpcTimeout());
    }

    @VisibleForTesting
    public RateBasedBackPressure(Map<String, Object> args, TimeSource timeSource, long windowSize)
    {
        if (args.size() != 3)
        {
            throw new IllegalArgumentException(RateBasedBackPressure.class.getCanonicalName()
                    + " requires 3 arguments: high ratio, back-pressure factor and flow type.");
        }

        try
        {
            highRatio = Double.parseDouble(args.getOrDefault(HIGH_RATIO, "").toString().trim());
            factor = Integer.parseInt(args.getOrDefault(FACTOR, "").toString().trim());
            flow = Flow.valueOf(args.getOrDefault(FLOW, "").toString().trim().toUpperCase());
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }

        if (highRatio <= 0 || highRatio > 1)
        {
            throw new IllegalArgumentException("Back-pressure high ratio must be > 0 and <= 1");
        }
        if (factor < 1)
        {
            throw new IllegalArgumentException("Back-pressure factor must be >= 1");
        }
        if (windowSize < 10)
        {
            throw new IllegalArgumentException("Back-pressure window size must be >= 10");
        }

        this.timeSource = timeSource;
        this.windowSize = windowSize;

        logger.info("Initialized back-pressure with high ratio: {}, factor: {}.", highRatio, factor);
    }

    @Override
    public void apply(Iterable<RateBasedBackPressureState> states)
    {
        SortedSet<RateBasedBackPressureState> sortedStates = new TreeSet<>((Comparator<RateBasedBackPressureState>) (s1, s2) -> 
        {
            if (s1.equals(s2))
                return 0;
            if (s1.getBackPressureRateLimit() < s2.getBackPressureRateLimit())
                return 1;
            if (s2.getBackPressureRateLimit() < s1.getBackPressureRateLimit())
                return -1;

            return s1.hashCode() - s2.hashCode();
        });
        
        for (RateBasedBackPressureState backPressure : states) 
        {
            if (backPressure.tryAcquireNewWindow(windowSize))
            {
                try
                {
                    RateLimiter limiter = backPressure.outgoingLimiter;

                    // Get the incoming/outgoing rates:
                    double incomingRate = backPressure.incomingRate.get(TimeUnit.SECONDS);
                    double outgoingRate = backPressure.outgoingRate.get(TimeUnit.SECONDS);
                    
                    // If we have sent any outgoing requests during this time window, go ahead with rate limiting
                    // (this is safe against concurrent back-pressure state updates thanks to the rw-locking in
                    // RateBasedBackPressureState):
                    if (outgoingRate > 0)
                    {
                        // Compute the incoming/outgoing ratio:
                        double actualRatio = incomingRate / outgoingRate;

                        // If the ratio is above the high mark, try growing by the back-pressure factor:
                        if (actualRatio >= highRatio)
                        {
                            // Only if the outgoing rate is able to keep up with the rate increase:
                            if (limiter.getRate() <= outgoingRate)
                            {
                                double newRate = limiter.getRate() + ((limiter.getRate() * factor) / 100);
                                if (newRate > 0 && newRate != Double.POSITIVE_INFINITY)
                                {
                                    limiter.setRate(newRate);
                                }
                            }
                        }
                        // If below, set the rate limiter at the incoming rate, decreased by factor:
                        else
                        {
                            // Only if the new rate is actually less than the actual rate:
                            double newRate = incomingRate - ((incomingRate * factor) / 100);
                            if (newRate > 0 && newRate < limiter.getRate())
                            {
                                limiter.setRate(newRate);
                            }
                        }

                        logger.debug("Back-pressure state for {}: incoming rate {}, outgoing rate {}, ratio {}, rate limiting {}",
                                     backPressure.getHost(), incomingRate, outgoingRate, actualRatio, limiter.getRate());
                    }
                    // Otherwise reset the rate limiter:
                    else
                    {
                        limiter.setRate(Double.POSITIVE_INFINITY);
                    }
                    
                    // Housekeeping: pruning windows and resetting the last check timestamp!
                    backPressure.incomingRate.prune();
                    backPressure.outgoingRate.prune();
                }
                finally
                {
                    backPressure.releaseWindow();
                }
            }
            sortedStates.add(backPressure);
        }
        
        if (!sortedStates.isEmpty())
            flow.get(sortedStates).doRateLimit();
    }
        
    @Override
    public RateBasedBackPressureState newState(InetAddress host)
    {
        return new RateBasedBackPressureState(host, timeSource, windowSize);
    }
}
