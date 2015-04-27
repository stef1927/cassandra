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

import java.util.UUID;

import org.junit.Test;

/**
 * Test column ranges and ordering with static column in table
 */
public class SelectTest extends CQLTester
{
    @Test
    public void testSingleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c))");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c =? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }

    @Test
    public void testSingleClusteringReversed() throws Throwable
    {
        createTable("CREATE TABLE %s (p text, c text, v text, s text static, PRIMARY KEY (p, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s(p, c, v, s) values (?, ?, ?, ?)", "p1", "k1", "v1", "sv1");
        execute("INSERT INTO %s(p, c, v) values (?, ?, ?)", "p1", "k2", "v2");
        execute("INSERT INTO %s(p, s) values (?, ?)", "p2", "sv2");

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=?", "p2"),
            row("p2", null, "sv2", null)
        );

        // Ascending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c ASC", "p2"),
            row("p2", null, "sv2", null)
        );

        // Descending order

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? ORDER BY c DESC", "p2"),
            row("p2", null, "sv2", null)
        );

        // No order with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=?", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=?", "p1", "k0"));

        // Ascending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c ASC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c ASC", "p1", "k0"));

        // Descending with one relation

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k2"),
            row("p1", "k2", "sv1", "v2")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c>=? ORDER BY c DESC", "p1", "k3"));

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k1"),
            row("p1", "k1", "sv1", "v1")
        );

        assertEmpty(execute("SELECT * FROM %s WHERE p=? AND c<=? ORDER BY c DESC", "p1", "k0"));

        // IN

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?)", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c ASC", "p1", "k1", "k2"),
            row("p1", "k1", "sv1", "v1"),
            row("p1", "k2", "sv1", "v2")
        );

        assertRows(execute("SELECT * FROM %s WHERE p=? AND c IN (?, ?) ORDER BY c DESC", "p1", "k1", "k2"),
            row("p1", "k2", "sv1", "v2"),
            row("p1", "k1", "sv1", "v1")
        );
    }

    /**
     * Check query with KEY IN clause
     * migrated from cql_tests.py:TestCQL.select_key_in_test()
     */
    @Test
    public void testSelectKeyIn() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Frodo', 'Baggins', 32)", id1);
        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, 'Samwise', 'Gamgee', 33)", id2);

        assertRowCount(execute("SELECT firstname, lastname FROM %s WHERE userid IN (?, ?)", id1, id2), 2);
    }

    /**
     * Check query with KEY IN clause for wide row tables
     * migrated from cql_tests.py:TestCQL.in_clause_wide_rows_test()
     */
    @Test
    public void testSelectKeyInForWideRows() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c IN (5, 2, 8)"),
                   row(2), row(5), row(8));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, ?, ?)", i, i);

        assertEmpty(execute("SELECT v FROM %s WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3"));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c1 = 0 AND c2 IN (5, 2, 8)"),
                   row(2), row(5), row(8));
    }

    /**
     * Check SELECT respects inclusive and exclusive bounds
     * migrated from cql_tests.py:TestCQL.select_key_in_test()
     */
    @Test
    public void testSelectBounds() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c)) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (0, ?, ?)", i, i);

        assertRowCount(execute("SELECT v FROM %s WHERE k = 0"), 10);

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c <= 6"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6"),
                   row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6"),
                   row(2), row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c < 6"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2"),
                   row(3), row(4));

        assertRows(execute("SELECT v FROM %s WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"),
                   row(5), row(4));
    }
}
