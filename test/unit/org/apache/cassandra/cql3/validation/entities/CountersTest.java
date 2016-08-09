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

package org.apache.cassandra.cql3.validation.entities;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.assertEquals;

public class CountersTest extends CQLTester
{
    /**
     * Check for a table with counters,
     * migrated from cql_tests.py:TestCQL.counters_test()
     */
    @Test
    public void testCounters() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, total counter, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        execute("UPDATE %s SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(1L));

        execute("UPDATE %s SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-3L));

        execute("UPDATE %s SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-2L));

        execute("UPDATE %s SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'");
        assertRows(execute("SELECT total FROM %s WHERE userid = 1 AND url = 'http://foo.com'"),
                   row(-4L));
    }

    /**
     * Test for the validation bug of #4706,
     * migrated from cql_tests.py:TestCQL.validate_counter_regular_test()
     */
    @Test
    public void testRegularCounters() throws Throwable
    {
        assertInvalidThrowMessage("Cannot add a non counter column",
                                  ConfigurationException.class,
                                  String.format("CREATE TABLE %s.%s (id bigint PRIMARY KEY, count counter, things set<text>)", KEYSPACE, createTableName()));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_counter_test()
     */
    @Test
    public void testCountersOnCollections() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, l list<counter>)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, s set<counter>)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalidThrow(InvalidRequestException.class,
                           String.format("CREATE TABLE %s (k int PRIMARY KEY, m map<text, counter>)", tableName));
    }

    @Test
    public void testMultipleThreads() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, c counter)");


        final BlockingQueue<Pair<Integer, Long>> expectedValues = new LinkedBlockingDeque<>();
        final AtomicInteger numErrors = new AtomicInteger(0);
        Thread readThread = new Thread(new Runnable()
        {
            public void run()
            {
                try
                {
                    while(true)
                    {
                        Pair<Integer, Long> expected = expectedValues.take();
                        if (expected.getLeft() == -1)
                            break;

                        try
                        {
                            assertRows(execute("SELECT * from %s WHERE id = ?", expected.getLeft()),
                                       row(expected.getLeft(), expected.getRight()));
                        }
                        catch (Throwable t)
                        {
                            numErrors.incrementAndGet();
                            logger.error("Failed to read counter for {}", expected, t);
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    numErrors.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });
        readThread.start();

        for (int val = 1; val < 50; val++)
        {
            while (!expectedValues.isEmpty())
                Thread.sleep(1);

            for (int id = 0; id < 500; id++)
            {
                execute("UPDATE %s SET c = c + 1 WHERE id = ?", id);
                expectedValues.put(Pair.of(id, (long)val));
            }
        }

        expectedValues.put(Pair.of(-1, 0L));
        readThread.join();

        assertEquals(0, numErrors.get());
    }
}
