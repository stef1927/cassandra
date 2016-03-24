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

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import junit.framework.Assert;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.transport.Server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BulkReadTest extends CQLTester
{
    int protocolVersion = Server.CURRENT_VERSION;

    @BeforeClass
    public static void startup()
    {
        requireNetwork();
    }

    @Test
    public void selectEntireTable1KB() throws Throwable
    {
        createTable(1000, 1, 1024);

        System.out.println(String.format("Total time reading 1KB table with streaming: %d milliseconds",
                                         readEntireTableWithStreaming(100, 100, 1000)));
        System.out.println(String.format("Total time reading 1KB table page by page: %d milliseconds",
                                         readEntireTablePageByPage(100, 100, 1000)));
    }

    @Test
    public void selectEntireTable10KB() throws Throwable
    {
        createTable(1000, 1, 1024*1024);
        System.out.println(String.format("Total time reading 10KB table page by page: %d milliseconds",
                                         readEntireTablePageByPage(10, 100, 1000)));
        System.out.println(String.format("Total time reading 10KB table with streaming: %d milliseconds",
                                         readEntireTableWithStreaming(10, 100, 1000)));
    }

    @Test
    public void selectEntireTable100B() throws Throwable
    {
        createTable(1000, 100, 50);
        System.out.println(String.format("Total time reading 100B table page by page: %d milliseconds",
                                         readEntireTablePageByPage(100, 5000, 100000)));
        System.out.println(String.format("Total time reading 100B table with streaming: %d milliseconds",
                                         readEntireTableWithStreaming(100, 5000, 100000)));
    }

    private void createTable(int numPartitions, int numClusterings, int partitionSize) throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, val1 TEXT, val2 TEXT, PRIMARY KEY(k, c))");

        //Object[][] rows = new Object[numPartitions * numClusterings][];
        int valueSize = partitionSize / numClusterings / 2;
        String text1 = generateText(valueSize);
        String text2 = generateText(valueSize);

        for (int i = 0; i < numPartitions; i++)
        {
            for (int j = 0; j < numClusterings; j++)
            {
                execute("INSERT INTO %s (k, c, val1, val2) VALUES (?, ?, ?, ?)", i, j, text1, text2);
                //rows[i * numClusterings + j] = row(i, j, text1, text2);
            }
        }

        System.out.println("Finished writing.");

        flush();
        System.out.println("Finished flushing.");

        compact();
        System.out.println("Finished compacting.");
    }

    private String generateText(int size)
    {
        Random rnd = new Random();
        char chars[] = new char[size];

        for (int i = 0; i < size; )
            for (long v = rnd.nextLong(),
                 n = Math.min(size - i, Long.SIZE/Byte.SIZE);
                 n-- > 0; v >>= Byte.SIZE)
                chars[i++] = (char) (((v & 127) + 32) & 127);
        return new String(chars, 0, size);
    }

    private long readEntireTablePageByPage(int numTrials, int pageSize, int numExpectedRows) throws Throwable
    {
        long start = System.nanoTime();
        for (int i = 0; i < numTrials; i++)
        {
            Session session = sessionNet(protocolVersion);
            Statement statement = new SimpleStatement(formatQuery("SELECT k, c, val1, val2 FROM %s"));
            statement.setFetchSize(pageSize);

            List<CompletableFuture<Integer>> results = new ArrayList<>();
            ListenableFuture<ResultSet> resultFuture = session.executeAsync(statement);
            while (resultFuture != null)
            {
                final ResultSet resultSet = resultFuture.get();
                if (!resultSet.isFullyFetched()) // the best we can do here is start fetching before processing
                    resultFuture = resultSet.fetchMoreResults(); // this batch of results
                else
                    resultFuture = null;

                results.add(CompletableFuture.supplyAsync(() -> processResultSet(resultSet)));
            }

            Optional<Integer> numRows = results.stream().map(this::extractResult).reduce(Integer::sum);

            assertTrue(numRows.isPresent());
            assertEquals(numExpectedRows, (int)numRows.get());
        }

        return (System.nanoTime() - start) / (1000000 * numTrials);
    }

    private long readEntireTableWithStreaming(int numTrials, int pageSize, int numExpectedRows) throws Throwable
    {
        long start = System.nanoTime();
        for (int i = 0; i < numTrials; i++)
        {
            Session session = sessionNet(protocolVersion);
            Statement statement = new SimpleStatement(formatQuery("SELECT k, c, val1, val2 FROM %s"));
            statement.setFetchSize(pageSize);


            Iterator<ResultSetFuture> it = session.streamAsync(statement);

            //Schedule the conversions
            List<CompletableFuture<Integer>> results = new ArrayList<>();
            while (it.hasNext())
            {
                ResultSetFuture rsf = it.next(); // Note: it is important to get this one here, synchronously
                results.add(CompletableFuture.supplyAsync(() -> processResultSetFuture(rsf)));
            }

            //Now wait for the futures to complete and extract the results
            Optional<Integer> numRows = results.stream().map(this::extractResult).reduce(Integer::sum);

            assertTrue(numRows.isPresent());
            assertEquals(numExpectedRows, (int)numRows.get());
        }

        return (System.nanoTime() - start) / (1000000 * numTrials);
    }

    private int processResultSetFuture(ResultSetFuture resultSetFuture)
    {
        try
        {
            return processResultSet(resultSetFuture.get());
        }
        catch (Exception ex)
        {
            fail(ex.getMessage());
            return 0;
        }
    }

    private int extractResult(CompletableFuture<Integer> cf)
    {
        try
        {
            return cf.get();
        }
        catch (Exception ex)
        {
            fail(ex.getMessage());
            return 0;
        }
    }

    //simulates performing some processing with the results
    private int processResultSet(ResultSet resultSet)
    {
        int numRows = resultSet.getAvailableWithoutFetching();
        for (int i = 0; i < numRows; i++)
        {
            assertTrue(resultSet.iterator().hasNext());
            assertNotNull(resultSet.iterator().next());
        }

        return numRows;
    }

}
