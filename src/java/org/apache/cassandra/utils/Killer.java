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
package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.service.StorageService;

/**
 * Responsible for killing the process
 */
public final class Killer
{
    private static final Logger logger = LoggerFactory.getLogger(Killer.class);
    private static Impl impl = new Impl();

    private Killer() {}

    public static void kill(Throwable t)
    {
        impl.kill(t);
    }

    @VisibleForTesting
    public static Impl replaceImpl(Impl newImpl) {
        Impl oldImpl = Killer.impl;
        Killer.impl = newImpl;
        return oldImpl;
    }

    @VisibleForTesting
    public static class Impl
    {
        /**
         * Certain situations represent "Die" conditions for the server, and if so, the reason is logged and the current JVM is killed.
         *
         * @param t : The Throwable to log before killing the current JVM
         */
        protected void kill(Throwable t)
        {
            t.printStackTrace(System.err);
            logger.error("Exiting forcefully due to:", t);
            StorageService.instance.removeShutdownHook();
            System.exit(100);
        }
    }
}

