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
package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link GossipingPropertyFileSnitch}.
 */
public class GossipingPropertyFileSnitchTest
{
    private Path effectiveFile;
    private Path backupFile;
    private Path modifiedFile;

    IPartitioner partitioner;
    VersionedValue.VersionedValueFactory valueFactory;
    InetAddress host;
    Set<Token> tokens;

    @Before
    public void setup() throws ConfigurationException, IOException
    {
        String confFile = FBUtilities.resourceToFile(SnitchProperties.RACKDC_PROPERTY_FILENAME);

        effectiveFile = Paths.get(confFile);
        backupFile = Paths.get(confFile + ".bak");
        modifiedFile = Paths.get(confFile + ".mod");

        restoreOrigConfigFile();

        partitioner = new RandomPartitioner();
        valueFactory = new VersionedValue.VersionedValueFactory(partitioner);
        host = FBUtilities.getBroadcastAddress();
        tokens = Collections.singleton(partitioner.getRandomToken());

        Gossiper.instance.initializeNodeUnsafe(host, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(host, ApplicationState.TOKENS, valueFactory.tokens(tokens));

        setNodeShutdown();
    }

    private void restoreOrigConfigFile() throws IOException
    {
        if (Files.exists(backupFile))
        {
            Files.copy(backupFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            Files.delete(backupFile);
        }
    }

    private void setNodeShutdown()
    {
        StorageService.instance.getTokenMetadata().removeEndpoint(host);
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.shutdown(true));
        Gossiper.instance.markDead(host, Gossiper.instance.getEndpointStateForEndpoint(host));
    }

    private void setNodeLive()
    {
        Gossiper.instance.injectApplicationState(host, ApplicationState.STATUS, valueFactory.normal(tokens));
        Gossiper.instance.realMarkAlive(host, Gossiper.instance.getEndpointStateForEndpoint(host));
        StorageService.instance.getTokenMetadata().updateNormalTokens(tokens, host);
    }

    /**
     * Reloading of the configuraiton file should only succeed if the node is NOT live, see CASSANDRA-10243.
     */
    @Test
    public void testAutoReloadConfig() throws Exception
    {
        final GossipingPropertyFileSnitch snitch = new GossipingPropertyFileSnitch(/*refreshPeriodInSeconds*/1);
        YamlFileNetworkTopologySnitchTest.checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

        try
        {
            setNodeLive();

            Files.copy(effectiveFile, backupFile);
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            Thread.sleep(1500);
            YamlFileNetworkTopologySnitchTest.checkEndpoint(snitch, host.getHostAddress(), "DC1", "RAC1");

            setNodeShutdown();
            Files.copy(modifiedFile, effectiveFile, java.nio.file.StandardCopyOption.REPLACE_EXISTING);

            Thread.sleep(1500);
            YamlFileNetworkTopologySnitchTest.checkEndpoint(snitch, host.getHostAddress(), "DC2", "RAC2");
        }
        finally
        {
            restoreOrigConfigFile();
            setNodeShutdown();
        }
    }
}
