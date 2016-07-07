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

import java.util.Map;
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
public class RateBasedBackPressure implements BackPressureStrategy
{
    public static final String HIGH_RATIO = "high_ratio";
    public static final String LOW_RATIO = "low_ratio";
    public static final String FACTOR = "factor";
    private static final String BACK_PRESSURE_HIGH_RATIO = "0.90";
    private static final String BACK_PRESSURE_LOW_RATIO = "0.10";
    private static final String BACK_PRESSURE_FACTOR = "5";

    private static final Logger logger = LoggerFactory.getLogger(RateBasedBackPressure.class);
    private final TimeSource timeSource;
    private final double highRatio;
    private final double lowRatio;
    private final int factor;
    private final long windowSize;

    public static ParameterizedClass withDefaultParams()
    {
        return new ParameterizedClass(RateBasedBackPressure.class.getName(),
                                      ImmutableMap.of(HIGH_RATIO, BACK_PRESSURE_HIGH_RATIO,
                                                      LOW_RATIO, BACK_PRESSURE_LOW_RATIO,
                                                      FACTOR, BACK_PRESSURE_FACTOR));
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
                    + " requires 3 arguments: high ratio, low ratio, back-pressure factor.");
        }

        try
        {
            highRatio = Double.parseDouble(args.getOrDefault(HIGH_RATIO, "").toString().trim());
            lowRatio = Double.parseDouble(args.getOrDefault(LOW_RATIO, "").toString().trim());
            factor = Integer.parseInt(args.getOrDefault(FACTOR, "").toString().trim());
        }
        catch (Exception ex)
        {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }

        if (highRatio <= 0 || highRatio > 1)
        {
            throw new IllegalArgumentException("Back-pressure high ratio must be > 0 and <= 1");
        }
        if (lowRatio <= 0 || lowRatio > 1)
        {
            throw new IllegalArgumentException("Back-pressure low ratio must be > 0 and <= 1");
        }
        if (highRatio <= lowRatio)
        {
            throw new IllegalArgumentException("Back-pressure low ratio must be smaller than high ratio");
        }
        if (factor < 1)
        {
            throw new IllegalArgumentException("Back-pressure factor must be >= 1");
        }

        this.timeSource = timeSource;
        this.windowSize = windowSize;

        logger.info("Initialized back-pressure with high ratio: {}, low ratio: {}, factor: {}.", highRatio, lowRatio, factor);
    }

    @Override
    public void apply(BackPressureState backPressure)
    {
        RateLimiter limiter = backPressure.outgoingLimiter;

        if (backPressure.tryAcquireNewWindow(backPressure.windowSize))
        {
            try
            {
                // Get the incoming/outgoing rates:
                double incomingRate = backPressure.incomingRate.get(TimeUnit.SECONDS);
                double outgoingRate = backPressure.outgoingRate.get(TimeUnit.SECONDS);
                // Compute the incoming/outgoing ratio:
                double actualRatio = outgoingRate > 0 ? incomingRate / outgoingRate : 1;

                // First thing check if previously overloaded: clear the overload flag (we don't want to flood the
                // client with errors) and try limiting at a very reduced rate:
                if (backPressure.overload.get())
                {
                    limiter.setRate(Math.max(1, incomingRate));

                    backPressure.overload.set(false);
                }
                // Else...
                // If the ratio is above the high mark, try growing by the back-pressure factor:
                else if (actualRatio >= highRatio)
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

                    backPressure.overload.set(false);
                }
                // If in between low and high marks, set the rate limiter at the incoming rate, reduced by the actual
                // ratio to give time to recover:
                else if (actualRatio >= lowRatio && actualRatio < highRatio)
                {
                    // Only if the new rate is actually less than the actual rate:
                    double newRate = incomingRate * actualRatio;
                    if (newRate < limiter.getRate())
                    {
                        limiter.setRate(newRate);
                    }

                    backPressure.overload.set(false);
                }
                // Finally if just below the low ratio, set the overload flag:
                else
                {
                    backPressure.overload.set(true);
                }

                // Housekeeping: pruning windows and resetting the last check timestamp!
                backPressure.incomingRate.prune();
                backPressure.outgoingRate.prune();

                logger.debug("Back-pressure enabled with: incoming rate {}, outgoing rate {}, ratio {}, rate limiting {}",
                             incomingRate, outgoingRate, actualRatio, limiter.getRate());
            }
            finally
            {
                backPressure.releaseWindow();
            }
        }

        // At this point we're either overloaded or rate limited:
        if (!backPressure.overload.get())
        {
            limiter.acquire(1);
        }
    }

    @Override
    public BackPressureState newState()
    {
        return new BackPressureState(timeSource, windowSize);
    }
}
