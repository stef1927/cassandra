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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import junit.framework.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TableTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitioner(new Murmur3Partitioner());
    }

    @Test
    public void testCQL3PartitionKeyOnlyTable()
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY);");
        assertFalse(currentTableMetadata().isThriftCompatible());
    }

    /**
     * Creation and basic operations on a static table,
     * migrated from cql_tests.py:TestCQL.static_cf_test()
     */
    @Test
    public void testStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int)");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

    /**
     * Creation and basic operations on a static table with compact storage,
     * migrated from cql_tests.py:TestCQL.noncomposite_static_cf_test()
     */
    @Test
    public void testDenseStaticTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid PRIMARY KEY, firstname text, lastname text, age int) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, firstname, lastname, age) VALUES (?, ?, ?, ?)", id1, "Frodo", "Baggins", 32);
        execute("UPDATE %s SET firstname = ?, lastname = ?, age = ? WHERE userid = ?", "Samwise", "Gamgee", 33, id2);

        assertRows(execute("SELECT firstname, lastname FROM %s WHERE userid = ?", id1),
                   row("Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id1),
                   row(id1, 32, "Frodo", "Baggins"));

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 33, "Samwise", "Gamgee"),
                   row(id1, 32, "Frodo", "Baggins")
        );

        String batch = "BEGIN BATCH "
                       + "INSERT INTO %1$s (userid, age) VALUES (?, ?) "
                       + "UPDATE %1$s SET age = ? WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "DELETE firstname, lastname FROM %1$s WHERE userid = ? "
                       + "APPLY BATCH";

        execute(batch, id1, 36, 37, id2, id1, id2);

        assertRows(execute("SELECT * FROM %s"),
                   row(id2, 37, null, null),
                   row(id1, 36, null, null));
    }

    /**
     * Creation and basic operations on a non-composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dynamic_cf_test()
     */
    @Test
    public void testDenseNonCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");
        UUID id3 = UUID.fromString("810e8500-e29b-41d4-a716-446655440000");

        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo.bar", 42L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://foo-2.bar", 24L);
        execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", id1, "http://bar.bar", 128L);
        execute("UPDATE %s SET time = 24 WHERE userid = ? and url = 'http://bar.foo'", id2);
        execute("UPDATE %s SET time = 12 WHERE userid IN (?, ?) and url = 'http://foo-3'", id2, id1);

        assertRows(execute("SELECT url, time FROM %s WHERE userid = ?", id1),
                   row("http://bar.bar", 128L),
                   row("http://foo-2.bar", 24L),
                   row("http://foo-3", 12L),
                   row("http://foo.bar", 42L));

        assertRows(execute("SELECT * FROM %s WHERE userid = ?", id2),
                   row(id2, "http://bar.foo", 24L),
                   row(id2, "http://foo-3", 12L));

        assertRows(execute("SELECT time FROM %s"),
                   row(24L), // id2
                   row(12L),
                   row(128L), // id1
                   row(24L),
                   row(12L),
                   row(42L)
        );

        // Check we don't allow empty values for url since this is the full underlying cell name (#6152)
        assertInvalid("INSERT INTO %s (userid, url, time) VALUES (?, '', 42)", id3);
    }

    /**
     * Creation and basic operations on a composite table with compact storage,
     * migrated from cql_tests.py:TestCQL.dense_cf_test()
     */
    @Test
    public void testDenseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, ip text, port int, time bigint, PRIMARY KEY (userid, ip, port)) WITH COMPACT STORAGE");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.1', 80, 42)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 80, 24)", id1);
        execute("INSERT INTO %s (userid, ip, port, time) VALUES (?, '192.168.0.2', 90, 42)", id1);
        execute("UPDATE %s SET time = 24 WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id2);

        // we don't have to include all of the clustering columns (see CASSANDRA-7990)
        execute("INSERT INTO %s (userid, ip, time) VALUES (?, '192.168.0.3', 42)", id2);
        execute("UPDATE %s SET time = 42 WHERE userid = ? AND ip = '192.168.0.4'", id2);

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ?", id1),
                   row("192.168.0.1", 80, 42L),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip >= '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip = '192.168.0.2'", id1),
                   row("192.168.0.2", 80, 24L),
                   row("192.168.0.2", 90, 42L));

        assertEmpty(execute("SELECT ip, port, time FROM %s WHERE userid = ? and ip > '192.168.0.2'", id1));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2),
                   row("192.168.0.3", null, 42L));

        assertRows(execute("SELECT ip, port, time FROM %s WHERE userid = ? AND ip = '192.168.0.4'", id2),
                   row("192.168.0.4", null, 42L));

        execute("DELETE time FROM %s WHERE userid = ? AND ip = '192.168.0.2' AND port = 80", id1);

        assertRowCount(execute("SELECT * FROM %s WHERE userid = ?", id1), 2);

        execute("DELETE FROM %s WHERE userid = ?", id1);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ?", id1));

        execute("DELETE FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2);
        assertEmpty(execute("SELECT * FROM %s WHERE userid = ? AND ip = '192.168.0.3'", id2));
    }

    /**
     * Creation and basic operations on a composite table,
     * migrated from cql_tests.py:TestCQL.sparse_cf_test()
     */
    @Test
    public void testSparseCompositeTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid uuid, posted_month int, posted_day int, body text, posted_by text, PRIMARY KEY (userid, posted_month, posted_day))");

        UUID id1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");
        UUID id2 = UUID.fromString("f47ac10b-58cc-4372-a567-0e02b2c3d479");

        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 12, 'Something else', 'Frodo Baggins')", id1);
        execute("INSERT INTO %s (userid, posted_month, posted_day, body, posted_by) VALUES (?, 1, 24, 'Something something', 'Frodo Baggins')", id1);
        execute("UPDATE %s SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = ? AND posted_month = 1 AND posted_day = 3", id2);
        execute("UPDATE %s SET body = 'Yet one more message' WHERE userid = ? AND posted_month = 1 and posted_day = 30", id1);

        assertRows(execute("SELECT body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day = 24", id1),
                   row("Something something", "Frodo Baggins"));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1 AND posted_day > 12", id1),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));

        assertRows(execute("SELECT posted_day, body, posted_by FROM %s WHERE userid = ? AND posted_month = 1", id1),
                   row(12, "Something else", "Frodo Baggins"),
                   row(24, "Something something", "Frodo Baggins"),
                   row(30, "Yet one more message", null));
    }

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
     * Check invalid create table statements,
     * migrated from cql_tests.py:TestCQL.create_invalid_test()
     */
    @Test
    public void testInvalidCreateTableStatements() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test ()");

        assertInvalid("CREATE TABLE test (c1 text, c2 text, c3 text)");
        assertInvalid("CREATE TABLE test (key1 text PRIMARY KEY, key2 text PRIMARY KEY)");

        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, key int)");
        assertInvalid("CREATE TABLE test (key text PRIMARY KEY, c int, c text)");

        assertInvalid("CREATE TABLE test (key text, key2 text, c int, d text, PRIMARY KEY (key, key2)) WITH COMPACT STORAGE");
    }

    /**
     * Check obsolete properties from CQL2 are rejected
     * migrated from cql_tests.py:TestCQL.invalid_old_property_test()
     */
    @Test
    public void testObsoleteTableProperties() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE TABLE test (foo text PRIMARY KEY, c int) WITH default_validation=timestamp");

        createTable("CREATE TABLE %s (foo text PRIMARY KEY, c int)");
        assertInvalidThrow(SyntaxException.class, "ALTER TABLE %s WITH default_validation=int");
    }

    /**
     * Test support for nulls
     * migrated from cql_tests.py:TestCQL.null_support_test()
     */
    @Test
    public void testNullSupport() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 set<text>, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v1, v2) VALUES (0, 0, null, {'1', '2'})");
        execute("INSERT INTO %s (k, c, v1) VALUES (0, 1, 1)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null, set("1", "2")),
                   row(0, 1, 1, null));

        execute("INSERT INTO %s (k, c, v1) VALUES (0, 1, null)");
        execute("INSERT INTO %s (k, c, v2) VALUES (0, 0, null)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0, null, null),
                   row(0, 1, null, null));

        assertInvalid("INSERT INTO %s (k, c, v2) VALUES (0, 2, {1, null})");
        assertInvalid("SELECT * FROM %s WHERE k = null");
        assertInvalid("INSERT INTO %s (k, c, v2) VALUES (0, 0, { 'foo', 'bar', null })");
    }


    /**
     * Test simple deletion and in particular check for #4193 bug
     * migrated from cql_tests.py:TestCQL.deletion_test()
     */
    @Test
    public void testDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (username varchar, id int, name varchar, stuff varchar, PRIMARY KEY(username, id))");

        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 2, "rst", "some value");
        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 4, "xyz", "some other value");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 2, "rst", "some value"),
                   row("abc", 4, "xyz", "some other value"));

        execute("DELETE FROM %s WHERE username='abc' AND id=2");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 4, "xyz", "some other value"));

        createTable("CREATE TABLE %s (username varchar, id int, name varchar, stuff varchar, PRIMARY KEY(username, id, name)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 2, "rst", "some value");
        execute("INSERT INTO %s (username, id, name, stuff) VALUES (?, ?, ?, ?)", "abc", 4, "xyz", "some other value");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 2, "rst", "some value"),
                   row("abc", 4, "xyz", "some other value"));

        execute("DELETE FROM %s WHERE username='abc' AND id=2");

        assertRows(execute("SELECT * FROM %s"),
                   row("abc", 4, "xyz", "some other value"));

        //# Won't be allowed until #3708 is in
        //if self.cluster.version() < "1.2":
        //assertInvalid("DELETE FROM testcf2 WHERE username='abc' AND id=2")
    }

    /**
     * Test select count
     * migrated from cql_tests.py:TestCQL.count_test()
     */
    @Test
    public void testSelectCount() throws Throwable
    {
        createTable(" CREATE TABLE %s (kind text, time int, value1 int, value2 int, PRIMARY KEY(kind, time))");

        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 0, 0, 0);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 2, 2);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev1', ?, ?, ?)", 3, 3, 3);
        execute("INSERT INTO %s (kind, time, value1) VALUES ('ev1', ?, ?)", 4, 4);
        execute("INSERT INTO %s (kind, time, value1, value2) VALUES ('ev2', 0, 0, 0)");

        assertRows(execute("SELECT COUNT(*) FROM %s WHERE kind = 'ev1'"),
                   row(5L));

        assertRows(execute("SELECT COUNT(1) FROM %s WHERE kind IN ('ev1', 'ev2') AND time=0"),
                   row(2L));
    }

    /**
     * Test reserved keywords
     * migrated from cql_tests.py:TestCQL.reserved_keyword_test()
     */
    @Test
    public void testReservedKeywords() throws Throwable
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY, count counter)");

        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, String.format("CREATE TABLE %s.%s (select text PRIMARY KEY, x int)", keyspace(), tableName));
    }

    /**
     * Test identifiers
     * migrated from cql_tests.py:TestCQL.identifier_test()
     */
    @Test
    public void testIdentifiers() throws Throwable
    {
        createTable("CREATE TABLE %s (key_23 int PRIMARY KEY, CoLuMn int)");

        execute("INSERT INTO %s (Key_23, Column) VALUES (0, 0)");
        execute("INSERT INTO %s (KEY_23, COLUMN) VALUES (0, 0)");

        assertInvalid("INSERT INTO %s (key_23, column, column) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, column, COLUMN) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, key_23, column) VALUES (0, 0, 0)");
        assertInvalid("INSERT INTO %s (key_23, KEY_23, column) VALUES (0, 0, 0)");

        String tableName = createTableName();
        assertInvalidThrow(SyntaxException.class, String.format("CREATE TABLE %s.%s (select int PRIMARY KEY, column int)", keyspace(), tableName));
    }

    /**
     * Test create and drop keyspace
     * migrated from cql_tests.py:TestCQL.keyspace_test()
     */
    @Test
    public void testKeyspace() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE %s testXYZ ");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        assertInvalid(
                     "CREATE KEYSPACE My_much_much_too_long_identifier_that_should_not_work WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        execute("DROP KEYSPACE testXYZ");
        assertInvalidThrow(ConfigurationException.class, "DROP KEYSPACE non_existing");

        execute("CREATE KEYSPACE testXYZ WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");

        // clean-up
        execute("DROP KEYSPACE testXYZ");
    }

    /**
     * Test create and drop table
     * migrated from cql_tests.py:TestCQL.table_test()
     */
    @Test
    public void testTable() throws Throwable
    {
        String table1 = createTable(" CREATE TABLE %s (k int PRIMARY KEY, c int)");
        createTable(" CREATE TABLE %s (k int, name int, value int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE ");
        createTable(" CREATE TABLE %s (k int, c int, PRIMARY KEY (k),)");

        // existing table (cannot test due to assertion)
        //assertInvalidThrow(AlreadyExistsException.class, String.format("CREATE TABLE %s (k int PRIMARY KEY, c int)", table3));

        String table4 = createTableName();

        // repeated column
        assertInvalidMessage("Multiple definition of identifier k", String.format("CREATE TABLE %s (k int PRIMARY KEY, c int, k text)", table4));

        // compact storage limitations
        assertInvalidThrow(SyntaxException.class,
                           String.format("CREATE TABLE %s (k int, name, int, c1 int, c2 int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE", table4));

        execute(String.format("DROP TABLE %s.%s", keyspace(), table1));

        createTable(String.format("CREATE TABLE %s.%s ( k int PRIMARY KEY, c1 int, c2 int, ) ", keyspace(), table1));
    }

    /**
     * Test truncate statement,
     * migrated from cql_tests.py:TestCQL.table_test().
     */
    @Test
    public void testTruncate() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int, name int, value int, PRIMARY KEY(k, name)) WITH COMPACT STORAGE ");
        execute("TRUNCATE %s");
    }

    /**
     * Test batch statements
     * migrated from cql_tests.py:TestCQL.batch_test()
     */
    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (userid text PRIMARY KEY, name text, password text)");

        String query = "BEGIN BATCH\n"
                       + "INSERT INTO %1$s (userid, password, name) VALUES ('user2', 'ch@ngem3b', 'second user');\n"
                       + "UPDATE %1$s SET password = 'ps22dhds' WHERE userid = 'user3';\n"
                       + "INSERT INTO %1$s (userid, password) VALUES ('user4', 'ch@ngem3c');\n"
                       + "DELETE name FROM %1$s WHERE userid = 'user1';\n"
                       + "APPLY BATCH;";

        execute(query);
    }

    /**
     * Test token ranges
     * migrated from cql_tests.py:TestCQL.token_range_test()
     */
    @Test
    public void testTokenRange() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, c int, v int)");

        int c = 100;
        for (int i = 0; i < c; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i);

        Object[][] res = getRows(execute("SELECT k FROM %s"));
        assertEquals(c, res.length);

        Object[] inOrder = new Object[res.length];
        for (int i = 0; i < res.length; i++)
            inOrder[i] = res[i][0];

        Long min_token = Long.MIN_VALUE;

        res = getRows(execute(String.format("SELECT k FROM %s.%s WHERE token(k) >= %d",
                                            keyspace(), currentTable(), min_token)));
        assertEquals(c, res.length);

        //assertInvalid("SELECT k FROM %s WHERE token(k) >= 0");
        //execute("SELECT k FROM %s WHERE token(k) >= 0");

        res = getRows(execute(String.format("SELECT k FROM %s.%s WHERE token(k) >= token(%d) AND token(k) < token(%d)",
                                            keyspace(), currentTable(), inOrder[32], inOrder[65])));

        for (int i = 32; i < 65; i++)
            Assert.assertEquals(inOrder[i], res[i - 32][0]);
    }

    /**
     * Test table options
     * migrated from cql_tests.py:TestCQL.table_options_test()
     */
    @Test
    public void testTableOptions() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int PRIMARY KEY, c int ) WITH "
                    + "comment = 'My comment' "
                    + "AND read_repair_chance = 0.5 "
                    + "AND dclocal_read_repair_chance = 0.5 "
                    + "AND gc_grace_seconds = 4 "
                    + "AND bloom_filter_fp_chance = 0.01 "
                    + "AND compaction = { 'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 10 } "
                    + "AND compression = { 'sstable_compression' : '' } "
                    + "AND caching = 'all' ");

        execute("ALTER TABLE %s WITH "
                + "comment = 'other comment' "
                + "AND read_repair_chance = 0.3 "
                + "AND dclocal_read_repair_chance = 0.3 "
                + "AND gc_grace_seconds = 100 "
                + "AND bloom_filter_fp_chance = 0.1 "
                + "AND compaction = { 'class': 'SizeTieredCompactionStrategy', 'min_sstable_size' : 42 } "
                + "AND compression = { 'sstable_compression' : 'SnappyCompressor' } "
                + "AND caching = 'rows_only' ");
    }

    /**
     * Test timestmp and ttl
     * migrated from cql_tests.py:TestCQL.timestamp_and_ttl_test()
     */
    @Test
    public void testTimestampTTL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c text, d text)");

        execute("INSERT INTO %s (k, c) VALUES (1, 'test')");
        execute("INSERT INTO %s (k, c) VALUES (2, 'test') USING TTL 400");

        Object[][] res = getRows(execute("SELECT k, c, writetime(c), ttl(c) FROM %s"));
        Assert.assertEquals(2, res.length);

        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }


        // wrap writetime(), ttl() in other functions (test for CASSANDRA-8451)
        res = getRows(execute("SELECT k, c, blobAsBigint(bigintAsBlob(writetime(c))), ttl(c) FROM %s"));
        Assert.assertEquals(2, res.length);

        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }

        res = getRows(execute("SELECT k, c, writetime(c), blobAsInt(intAsBlob(ttl(c))) FROM %s"));
        Assert.assertEquals(2, res.length);


        for (Object[] r : res)
        {
            assertTrue(r[2] instanceof Integer || r[2] instanceof Long);
            if (r[0].equals(1))
                assertNull(r[3]);
            else
                assertTrue(r[3] instanceof Integer || r[2] instanceof Long);
        }

        assertInvalid("SELECT k, c, writetime(k) FROM %s");

        assertRows(execute("SELECT k, d, writetime(d) FROM %s WHERE k = 1"),
                   row(1, null, null));
    }

    /**
     * Test deletions
     * migrated from cql_tests.py:TestCQL.no_range_ghost_test()
     */
    @Test
    public void testNoRangeGhost() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int PRIMARY KEY, v int ) ");

        for (int k = 0; k < 5; k++)
            execute("INSERT INTO %s (k, v) VALUES (?, 0)", k);

        Object[][] rows = getRows(execute("SELECT k FROM %s"));

        int[] ordered = sortIntRows(rows);
        for (int k = 0; k < 5; k++)
            assertEquals(k, ordered[k]);

        execute("DELETE FROM %s WHERE k=2");

        rows = getRows(execute("SELECT k FROM %s"));
        ordered = sortIntRows(rows);

        int idx = 0;
        for (int k = 0; k < 5; k++)
            if (k != 2)
                assertEquals(k, ordered[idx++]);

        // Example from #3505
        createTable("CREATE TABLE %s ( KEY varchar PRIMARY KEY, password varchar, gender varchar, birth_year bigint)");
        execute("INSERT INTO %s (KEY, password) VALUES ('user1', 'ch@ngem3a')");
        execute("UPDATE %s SET gender = 'm', birth_year = 1980 WHERE KEY = 'user1'");

        assertRows(execute("SELECT * FROM %s WHERE KEY='user1'"),
                   row("user1", 1980L, "m", "ch@ngem3a"));

        execute("TRUNCATE %s");
        assertEmpty(execute("SELECT * FROM %s"));

        assertEmpty(execute("SELECT * FROM %s WHERE KEY='user1'"));
    }

    private int[] sortIntRows(Object[][] rows)
    {
        int[] ret = new int[rows.length];
        for (int i = 0; i < ret.length; i++)
            ret[i] = rows[i][0] == null ? Integer.MIN_VALUE : (Integer) rows[i][0];
        Arrays.sort(ret);
        return ret;
    }

    /**
     * Test deletion by 'composite prefix' (range tombstones)
     * migrated from cql_tests.py:TestCQL.range_tombstones_test()
     */
    @Test
    public void testDeleteByCompositePrefix() throws Throwable
    { // This test used 3 nodes just to make sure RowMutation are correctly serialized

        createTable("CREATE TABLE %s ( k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2))");

        int numRows = 5;
        int col1 = 2;
        int col2 = 2;
        int cpr = col1 * col2;

        for (int i = 0; i < numRows; i++)
            for (int j = 0; j < col1; j++)
                for (int k = 0; k < col2; k++)
                {
                    int n = (i * cpr) + (j * col2) + k;
                    execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", i, j, k, n, n);
                }

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s where k = ?", i));
            for (int x = i * cpr; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr][0]);
                assertEquals(x, rows[x - i * cpr][1]);
            }
        }

        for (int i = 0; i < numRows; i++)
            execute("DELETE FROM %s WHERE k = ? AND c1 = 0", i);

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s WHERE k = ?", i));
            for (int x = i * cpr + col1; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr - col1][0]);
                assertEquals(x, rows[x - i * cpr - col1][1]);
            }
        }

        for (int i = 0; i < numRows; i++)
        {
            Object[][] rows = getRows(execute("SELECT v1, v2 FROM %s WHERE k = ?", i));
            for (int x = i * cpr + col1; x < (i + 1) * cpr; x++)
            {
                assertEquals(x, rows[x - i * cpr - col1][0]);
                assertEquals(x, rows[x - i * cpr - col1][1]);
            }
        }
    }

    /**
     * Test deletion by 'composite prefix' (range tombstones) with compaction
     * migrated from cql_tests.py:TestCQL.range_tombstones_compaction_test()
     */
    @Test
    public void testDeleteByCompositePrefixWithCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v1 text, PRIMARY KEY (k, c1, c2))");

        for (int c1 = 0; c1 < 4; c1++)
            for (int c2 = 0; c2 < 2; c2++)
                execute("INSERT INTO %s (k, c1, c2, v1) VALUES (0, ?, ?, ?)", c1, c2, String.format("%d%d", c1, c2));

        flush();

        execute("DELETE FROM %s WHERE k = 0 AND c1 = 1");

        flush();
        compact();

        Object[][] rows = getRows(execute("SELECT v1 FROM %s WHERE k = 0"));

        int idx = 0;
        for (int c1 = 0; c1 < 4; c1++)
            for (int c2 = 0; c2 < 2; c2++)
                if (c1 != 1)
                    assertEquals(String.format("%d%d", c1, c2), rows[idx++][0]);
    }

    /**
     * Test deletion of rows
     * migrated from cql_tests.py:TestCQL.delete_row_test()
     */
    @Test
    public void testRowDeletion() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v1 int, v2 int, PRIMARY KEY (k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 0, 0, 0);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 1, 1, 1);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 0, 2, 2, 2);
        execute("INSERT INTO %s (k, c1, c2, v1, v2) VALUES (?, ?, ?, ?, ?)", 0, 1, 0, 3, 3);

        execute("DELETE FROM %s WHERE k = 0 AND c1 = 0 AND c2 = 0");

        assertRowCount(execute("SELECT * FROM %s"), 3);
    }

    /**
     * Range test query from #4372
     * migrated from cql_tests.py:TestCQL.range_query_test()
     */
    @Test
    public void testRangeQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, e int, f text, PRIMARY KEY (a, b, c, d, e) )");

        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 2, '2')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 2, 1, '1')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 3, '3')");
        execute("INSERT INTO %s (a, b, c, d, e, f) VALUES (1, 1, 1, 1, 5, '5')");

        assertRows(execute("SELECT a, b, c, d, e, f FROM %s WHERE a = 1 AND b = 1 AND c = 1 AND d = 1 AND e >= 2"),
                   row(1, 1, 1, 1, 2, "2"),
                   row(1, 1, 1, 1, 3, "3"),
                   row(1, 1, 1, 1, 5, "5"));
    }

    /**
     * Test altering the type of a column, including the one in the primary key (#4041)
     * migrated from cql_tests.py:TestCQL.update_type_test()
     */
    @Test
    public void testUpdateType() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, s set <text>, v text, PRIMARY KEY(k, c))");

        // using utf8 character so that we can see the transition to BytesType
        execute("INSERT INTO %s (k, c, v, s) VALUES ('ɸ', 'ɸ', 'ɸ', {'ɸ'})");

        assertRows(execute("SELECT * FROM %s"),
                   row("ɸ", "ɸ", set("ɸ"), "ɸ"));

        execute("ALTER TABLE %s ALTER v TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row("ɸ", "ɸ", set("ɸ"), ByteBufferUtil.bytes("ɸ")));


        execute("ALTER TABLE %s ALTER k TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), "ɸ", set("ɸ"), ByteBufferUtil.bytes("ɸ")));

        execute("ALTER TABLE %s ALTER c TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), ByteBufferUtil.bytes("ɸ"), set("ɸ"), ByteBufferUtil.bytes("ɸ")));

        execute("ALTER TABLE %s ALTER s TYPE set<blob>");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), ByteBufferUtil.bytes("ɸ"), set(ByteBufferUtil.bytes("ɸ")), ByteBufferUtil.bytes("ɸ")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.composite_row_key_test()
     */
    @Test
    public void testCompositeRowKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, v int, PRIMARY KEY ((k1, k2), c))");

        for (int i = 0; i < 4; i++)
            execute("INSERT INTO %s (k1, k2, c, v) VALUES (?, ?, ?, ?)", 0, i, i, i);

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 2, 2, 2),
                   row(0, 3, 3, 3),
                   row(0, 0, 0, 0),
                   row(0, 1, 1, 1));

        assertRows(execute("SELECT * FROM %s WHERE k1 = 0 and k2 IN (1, 3)"),
                   row(0, 1, 1, 1),
                   row(0, 3, 3, 3));

        assertInvalid("SELECT * FROM %s WHERE k2 = 3");

        assertRows(execute("SELECT * FROM %s WHERE token(k1, k2) = token(0, 1)"),
                   row(0, 1, 1, 1));


        assertRows(execute("SELECT * FROM %s WHERE token(k1, k2) > ?", Long.MIN_VALUE),
                   row(0, 2, 2, 2),
                   row(0, 3, 3, 3),
                   row(0, 0, 0, 0),
                   row(0, 1, 1, 1));
    }

    /**
     * Check the semantic of CQL row existence (part of #4361),
     * migrated from cql_tests.py:TestCQL.row_existence_test()
     */
    @Test
    public void testRowExistence() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v1 int, v2 int, PRIMARY KEY (k, c))");

        execute("INSERT INTO %s (k, c, v1, v2) VALUES (1, 1, 1, 1)");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, 1));

        assertInvalid("DELETE c FROM %s WHERE k = 1 AND c = 1");

        execute("DELETE v2 FROM %s WHERE k = 1 AND c = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, 1, null));

        execute("DELETE v1 FROM %s WHERE k = 1 AND c = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 1, null, null));

        execute("DELETE FROM %s WHERE k = 1 AND c = 1");
        assertEmpty(execute("SELECT * FROM %s"));

        execute("INSERT INTO %s (k, c) VALUES (2, 2)");
        assertRows(execute("SELECT * FROM %s"),
                   row(2, 2, null, null));
    }

    /**
     * Check dates are correctly recognized and validated,
     * migrated from cql_tests.py:TestCQL.date_test()
     */
    @Test
    public void testDate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t timestamp)");

        execute("INSERT INTO %s (k, t) VALUES (0, '2011-02-03')");
        assertInvalid("INSERT INTO %s (k, t) VALUES (0, '2011-42-42')");
    }

    /**
     * Test a regression from #1337,
     * migrated from cql_tests.py:TestCQL.range_slice_test()
     */
    @Test
    public void testRangeSlice() throws Throwable
    { //This was using a two node cluster...
        createTable(" CREATE TABLE %s (k text PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) VALUES ('foo', 0)");
        execute("INSERT INTO %s (k, v) VALUES ('bar', 1)");

        assertRowCount(execute("SELECT * FROM %s"), 2);
    }

    /**
     * Test for #4532, NPE when trying to select a slice from a composite table
     * migrated from cql_tests.py:TestCQL.bug_4532_test()
     */
    @Test
    public void testSelectSliceFromComposite() throws Throwable
    {
        createTable("CREATE TABLE %s (status ascii, ctime bigint, key ascii, nil ascii, PRIMARY KEY (status, ctime, key))");

        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key1','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345678,'key2','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key3','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key4','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345679,'key5','')");
        execute("INSERT INTO %s (status,ctime,key,nil) VALUES ('C',12345680,'key6','')");

        assertInvalid("SELECT * FROM %s WHERE ctime>=12345679 AND key='key3' AND ctime<=12345680 LIMIT 3;");
        assertInvalid("SELECT * FROM %s WHERE ctime=12345679  AND key='key3' AND ctime<=12345680 LIMIT 3");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.create_alter_options_test()
     */
    @Test
    public void testCreateAlterKeyspaces() throws Throwable
    {
        assertInvalidThrow(SyntaxException.class, "CREATE KEYSPACE ks1");
        assertInvalidThrow(ConfigurationException.class, "CREATE KEYSPACE ks1 WITH replication= { 'replication_factor' : 1 }");

        execute("CREATE KEYSPACE ks1 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }");
        execute("CREATE KEYSPACE ks2 WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND durable_writes=false");

        assertRows(execute("SELECT keyspace_name, durable_writes FROM system.schema_keyspaces"),
                   row("ks1", true),
                   row(KEYSPACE, true),
                   row("ks2", false));

        execute("ALTER KEYSPACE ks1 WITH replication = { 'class' : 'NetworkTopologyStrategy', 'dc1' : 1 } AND durable_writes=False");
        execute("ALTER KEYSPACE ks2 WITH durable_writes=true");

        assertRows(execute("SELECT keyspace_name, durable_writes, strategy_class FROM system.schema_keyspaces"),
                   row("ks1", false, "org.apache.cassandra.locator.NetworkTopologyStrategy"),
                   row(KEYSPACE, true, "org.apache.cassandra.locator.SimpleStrategy"),
                   row("ks2", true, "org.apache.cassandra.locator.SimpleStrategy"));

        execute("USE ks1");

        assertInvalidThrow(ConfigurationException.class, "CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'min_threshold' : 4 }");

        execute("CREATE TABLE cf1 (a int PRIMARY KEY, b int) WITH compaction = { 'class' : 'SizeTieredCompactionStrategy', 'min_threshold' : 7 }");
        assertRows(execute("SELECT columnfamily_name, min_compaction_threshold FROM system.schema_columnfamilies WHERE keyspace_name='ks1'"),
                   row("cf1", 7));

        // clean-up
        execute("DROP KEYSPACE ks1");
        execute("DROP KEYSPACE ks2");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.remove_range_slice_test()
     */
    @Test
    public void testRemoveRangeSlice() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);

        execute("DELETE FROM %s WHERE k = 1");
        assertRows(execute("SELECT * FROM %s"),
                   row(0, 0),
                   row(2, 2));
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
     * Test for #4716 bug and more generally for good behavior of ordering,
     * migrated from cql_tests.py:TestCQL.reversed_compact_test()
     */
    @Test
    public void testReverseCompact() throws Throwable
    {
        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(6), row(5), row(4), row(3), row(2));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));

        createTable("CREATE TABLE %s ( k text, c int, v int, PRIMARY KEY (k, c) ) WITH COMPACT STORAGE");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s(k, c, v) VALUES ('foo', ?, ?)", i, i);

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo'"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo'"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c ASC"),
                   row(3), row(4), row(5));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c ASC"),
                   row(2), row(3), row(4), row(5), row(6));

        assertRows(execute("SELECT c FROM %s WHERE c > 2 AND c < 6 AND k = 'foo' ORDER BY c DESC"),
                   row(5), row(4), row(3));

        assertRows(execute("SELECT c FROM %s WHERE c >= 2 AND c <= 6 AND k = 'foo' ORDER BY c DESC"),
                   row(6), row(5), row(4), row(3), row(2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.unescaped_string_test()
     */
    @Test
    public void testUnescapedString() throws Throwable
    {
        createTable("CREATE TABLE %s ( k text PRIMARY KEY, c text, )");

        //The \ in this query string is not forwarded to cassandra.
        //The ' is being escaped in python, but only ' is forwarded
        //over the wire instead of \'.
        assertInvalidThrow(SyntaxException.class, "INSERT INTO %s (k, c) VALUES ('foo', 'CQL is cassandra\'s best friend')");
    }

    /**
     * Test for the bug from #4760 and #4759,
     * migrated from cql_tests.py:TestCQL.reversed_compact_multikey_test()
     */
    @Test
    public void testReversedCompactMultikey() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c1 int, c2 int, value text, PRIMARY KEY(key, c1, c2) ) WITH COMPACT STORAGE AND CLUSTERING ORDER BY(c1 DESC, c2 DESC)");

        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                execute("INSERT INTO %s (key, c1, c2, value) VALUES ('foo', ?, ?, 'bar')", i, j);

        // Equalities
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1"),
                   row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 = 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0));

        // GT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 ASC, c2 ASC"),
                   row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 > 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 ASC"),
                   row(1, 0), row(1, 1), row(1, 2), row(2, 0), row(2, 1), row(2, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 >= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(2, 2), row(2, 1), row(2, 0), row(1, 2), row(1, 1), row(1, 0));

        // LT
        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 < 1 ORDER BY c1 DESC, c2 DESC"),
                   row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 ASC"),
                   row(0, 0), row(0, 1), row(0, 2), row(1, 0), row(1, 1), row(1, 2));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE key='foo' AND c1 <= 1 ORDER BY c1 DESC, c2 DESC"),
                   row(1, 2), row(1, 1), row(1, 0), row(0, 2), row(0, 1), row(0, 0));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_and_regular_test()
     */
    @Test
    public void testCollectionAndRegularColumns() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>, c int)");

        execute("INSERT INTO %s (k, l, c) VALUES(3, [0, 1, 2], 4)");
        execute("UPDATE %s SET l[0] = 1, c = 42 WHERE k = 3");
        assertRows(execute("SELECT l, c FROM %s WHERE k = 3"),
                   row(list(1, 1, 2), 42));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.batch_and_list_test()
     */
    @Test
    public void testBatchAndList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<int>)");

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l = l +[ 1 ] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [ 2 ] WHERE k = 0; " +
                "UPDATE %1$s SET l = l + [ 3 ] WHERE k = 0; " +
                "APPLY BATCH");

        assertRows(execute("SELECT l FROM %s WHERE k = 0"),
                   row(list(1, 2, 3)));

        execute("BEGIN BATCH " +
                "UPDATE %1$s SET l =[ 1 ] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [ 2 ] + l WHERE k = 1; " +
                "UPDATE %1$s SET l = [ 3 ] + l WHERE k = 1; " +
                "APPLY BATCH ");

        assertRows(execute("SELECT l FROM %s WHERE k = 1"),
                   row(list(3, 2, 1)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.boolean_test()
     */
    @Test
    public void testBoolean() throws Throwable
    {
        createTable("CREATE TABLE %s (k boolean PRIMARY KEY, b boolean)");

        execute("INSERT INTO %s (k, b) VALUES (true, false)");
        assertRows(execute("SELECT * FROM %s WHERE k = true"),
                   row(true, false));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multiordering_test()
     */
    @Test
    public void testMultiordering() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c1 int, c2 int, PRIMARY KEY (k, c1, c2) ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");

        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k, c1, c2) VALUES ('foo', ?, ?)", i, j);

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo'"),
                   row(0, 1), row(0, 0), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 DESC"),
                   row(0, 1), row(0, 0), row(1, 1), row(1, 0));

        assertRows(execute("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 DESC, c2 ASC"),
                   row(1, 0), row(1, 1), row(0, 0), row(0, 1));

        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 DESC");
        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c2 ASC");
        assertInvalid("SELECT c1, c2 FROM %s WHERE k = 'foo' ORDER BY c1 ASC, c2 ASC");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multiordering_validation_test()
     */
    @Test
    public void testMultiOrderingValidation() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c2 ASC, c1 DESC)", tableName));

        tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE test (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC, c3 DESC)", tableName));

        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 DESC)");
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2)) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_4882_test()
     */
    @Test
    public void testDifferentOrdering() throws Throwable
    {
        createTable(" CREATE TABLE %s ( k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2) ) WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC)");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 1, 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 1, 3, 3)");

        assertRows(execute("select * from %s where k = 0 limit 1"),
                   row(0, 0, 2, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_list_set_test()
     */
    @Test
    public void testMultipleLists() throws Throwable
    {
        createTable(" CREATE TABLE %s (k int PRIMARY KEY, l1 list<int>, l2 list<int>)");

        execute("INSERT INTO %s (k, l1, l2) VALUES (0, [1, 2, 3], [4, 5, 6])");
        execute("UPDATE %s SET l2[1] = 42, l1[1] = 24  WHERE k = 0");

        assertRows(execute("SELECT l1, l2 FROM %s WHERE k = 0"),
                   row(list(1, 24, 3), list(4, 42, 6)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.allow_filtering_test()
     */
    @Test
    public void testAllowFiltering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, v int, PRIMARY KEY (k, c))");

        for (int i = 0; i < 3; i++)
            for (int j = 0; j < 3; j++)
                execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, j, j);

        // Don't require filtering, always allowed
        String[] queries = new String[]
                           {
                           "SELECT * FROM %s WHERE k = 1",
                           "SELECT * FROM %s WHERE k = 1 AND c > 2",
                           "SELECT * FROM %s WHERE k = 1 AND c = 2"
                           };

        for (String q : queries)
        {
            execute(q);
            execute(q + " ALLOW FILTERING");
        }

        // Require filtering, allowed only with ALLOW FILTERING
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE c = 2",
                  "SELECT * FROM %s WHERE c > 2 AND c <= 4"
                  };

        for (String q : queries)
        {
            assertInvalid(q);
            execute(q + " ALLOW FILTERING");
        }

        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int,)");
        createIndex("CREATE INDEX ON %s (a)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i * 10, i * 100);

        // Don't require filtering, always allowed
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE k = 1",
                  "SELECT * FROM %s WHERE a = 20"
                  };

        for (String q : queries)
        {
            execute(q);
            execute(q + " ALLOW FILTERING");
        }

        // Require filtering, allowed only with ALLOW FILTERING
        queries = new String[]
                  {
                  "SELECT * FROM %s WHERE a = 20 AND b = 200"
                  };

        for (String q : queries)
        {
            assertInvalid(q);
            execute(q + " ALLOW FILTERING");
        }
    }

    /**
     * Migrated from cql_tests.py:TestCQL.range_with_deletes_test()
     */
    @Test
    public void testRandomDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int,)");

        int nb_keys = 30;
        int nb_deletes = 5;

        List<Integer> deletions = new ArrayList<>(nb_keys);
        for (int i = 0; i < nb_keys; i++)
        {
            execute("INSERT INTO %s (k, v) VALUES (?, ?)", i, i);
            deletions.add(i);
        }

        Collections.shuffle(deletions);

        for (int i = 0; i < nb_deletes; i++)
            execute("DELETE FROM %s WHERE k = ?", deletions.get(i));

        assertRowCount(execute("SELECT * FROM %s LIMIT ?", (nb_keys / 2)), nb_keys / 2);
    }

    /**
     * Test you can add columns in a table with collections (#4982 bug),
     * migrated from cql_tests.py:TestCQL.alter_with_collections_test()
     */
    @Test
    public void testAlterCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key int PRIMARY KEY, aset set<text>)");
        execute("ALTER TABLE %s ADD c text");
        execute("ALTER TABLE %s ADD alist list<text>");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_compact_test()
     */
    @Test
    public void testCompactCollections() throws Throwable
    {
        String tableName = KEYSPACE + "." + createTableName();
        assertInvalid(String.format("CREATE TABLE %s (user ascii PRIMARY KEY, mails list < text >) WITH COMPACT STORAGE;", tableName));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.collection_function_test()
     */
    @Test
    public void testFunctionsOnCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l set<int>)");

        assertInvalid("SELECT ttl(l) FROM %s WHERE k = 0");
        assertInvalid("SELECT writetime(l) FROM %s WHERE k = 0");
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

    /**
     * Test for bug from #5122,
     * migrated from cql_tests.py:TestCQL.composite_partition_key_validation_test()
     */
    @Test
    public void testSelectOnCompositeInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b text, c uuid, PRIMARY KEY ((a, b)))");

        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'aze', 4d481800-4c5f-11e1-82e0-3f484de45426)");
        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'ert', 693f5800-8acb-11e3-82e0-3f484de45426)");
        execute("INSERT INTO %s (a, b , c ) VALUES (1, 'opl', d4815800-2d8d-11e0-82e0-3f484de45426)");

        assertRowCount(execute("SELECT * FROM %s"), 3);
        assertInvalid("SELECT * FROM %s WHERE a=1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.multi_in_compact_non_composite_test()
     */
    @Test
    public void testMultiSelectsNonCompositeCompactStorage() throws Throwable
    {
        createTable("CREATE TABLE %s (key int, c int, v int, PRIMARY KEY (key, c)) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (key, c, v) VALUES (0, 0, 0)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 1, 1)");
        execute("INSERT INTO %s (key, c, v) VALUES (0, 2, 2)");

        assertRows(execute("SELECT * FROM %s WHERE key=0 AND c IN (0, 2)"),
                   row(0, 0, 0), row(0, 2, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.timeuuid_test()
     */
    @Test
    public void testTimeuuid() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, t timeuuid, PRIMARY KEY(k, t))");

        assertInvalidThrow(SyntaxException.class, "INSERT INTO %s (k, t) VALUES (0, 2012-11-07 18:18:22-0800)");

        for (int i = 0; i < 4; i++)
        {
            execute("INSERT INTO %s (k, t) VALUES (0, now())");
            Thread.sleep(1000);
        }

        Object[][] rows = getRows(execute("SELECT * FROM %s"));
        assertEquals(4, rows.length);

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t >= ?", rows[0][1]), 4);

        assertEmpty(execute("SELECT * FROM %s WHERE k = 0 AND t < ?", rows[0][1]));

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t > ? AND t <= ?", rows[0][1], rows[2][1]), 2);

        assertRowCount(execute("SELECT * FROM %s WHERE k = 0 AND t = ?", rows[0][1]), 1);

        assertInvalid("SELECT dateOf(k) FROM %s WHERE k = 0 AND t = ?", rows[0][1]);

        execute("SELECT dateOf(t), unixTimestampOf(t) FROM %s WHERE k = 0 AND t = ?", rows[0][1]);
        execute("SELECT t FROM %s WHERE k = 0 AND t > maxTimeuuid(1234567) AND t < minTimeuuid('2012-11-07 18:18:22-0800')");
        // not sure what to check exactly so just checking the query returns
    }

    /**
     * Migrated from cql_tests.py:TestCQL.float_with_exponent_test()
     */
    @Test
    public void testFloatWithExponent() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, d double, f float)");

        execute("INSERT INTO %s (k, d, f) VALUES (0, 3E+10, 3.4E3)");
        execute("INSERT INTO %s (k, d, f) VALUES (1, 3.E10, -23.44E-3)");
        execute("INSERT INTO %s (k, d, f) VALUES (2, 3, -2)");
    }

    /**
     * Test regression from #5189,
     * migrated from cql_tests.py:TestCQL.compact_metadata_test()
     */
    @Test
    public void testCompactMetadata() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, i int ) WITH COMPACT STORAGE");

        execute("INSERT INTO %s (id, i) VALUES (1, 2)");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.clustering_indexing_test()
     */
    @Test
    public void testIndexesOnClustering() throws Throwable
    {
        createTable("CREATE TABLE %s ( id1 int, id2 int, author text, time bigint, v1 text, v2 text, PRIMARY KEY ((id1, id2), author, time))");

        createIndex("CREATE INDEX ON %s (time)");
        execute("CREATE INDEX ON %s (id2)");

        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 0, 'A', 'A')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'bob', 1, 'B', 'B')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'bob', 2, 'C', 'C')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 0, 'tom', 0, 'D', 'D')");
        execute("INSERT INTO %s (id1, id2, author, time, v1, v2) VALUES(0, 1, 'tom', 1, 'E', 'E')");

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"),
                   row("B"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 1"),
                   row("C"), row("E"));

        assertRows(execute("SELECT v1 FROM %s WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 0"),
                   row("A"));

        // Test for CASSANDRA-8206
        execute("UPDATE %s SET v2 = null WHERE id1 = 0 AND id2 = 0 AND author = 'bob' AND time = 1");

        assertRows(execute("SELECT v1 FROM %s WHERE id2 = 0"),
                   row("A"), row("B"), row("D"));

        assertRows(execute("SELECT v1 FROM %s WHERE time = 1"),
                   row("B"), row("E"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.invalid_clustering_indexing_test()
     */
    @Test
    public void testIndexesOnClusteringInvalid() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY ((a, b))) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");

        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b)) WITH COMPACT STORAGE");
        assertInvalid("CREATE INDEX ON %s (a)");
        assertInvalid("CREATE INDEX ON %s (b)");
        assertInvalid("CREATE INDEX ON %s (c)");

        createTable("CREATE TABLE %s (a int, b int, c int static , PRIMARY KEY (a, b))");
        assertInvalid("CREATE INDEX ON %s (c)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.ticket_5230_test()
     */
    @Test
    public void testMultipleClausesOnPrimaryKey() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c text, v text, PRIMARY KEY(key, c))");

        execute("INSERT INTO %s (key, c, v) VALUES ('foo', '1', '1')");
        execute("INSERT INTO %s(key, c, v) VALUES ('foo', '2', '2')");
        execute("INSERT INTO %s(key, c, v) VALUES ('foo', '3', '3')");

        assertRows(execute("SELECT c FROM %s WHERE key = 'foo' AND c IN ('1', '2')"),
                   row("1"), row("2"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.conversion_functions_test()
     */
    @Test
    public void testConversionFunctions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, i varint, b blob)");

        execute("INSERT INTO %s (k, i, b) VALUES (0, blobAsVarint(bigintAsBlob(3)), textAsBlob('foobar'))");
        assertRows(execute("SELECT i, blobAsText(b) FROM %s WHERE k = 0"),
                   row(BigInteger.valueOf(3), "foobar"));
    }

    /**
     * Test for bug of 5232,
     * migrated from cql_tests.py:TestCQL.alter_bug_test()
     */
    @Test
    public void testAlterStatementWithAdd() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, t text)");

        execute("UPDATE %s SET t = '111' WHERE id = 1");

        execute("ALTER TABLE %s ADD l list<text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, "111"));

        execute("ALTER TABLE %s ADD m map<int, text>");
        assertRows(execute("SELECT * FROM %s"),
                   row(1, null, null, "111"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_5376()
     */
    @Test
    public void testInClauseWithCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (key text, c bigint, v text, x set < text >, PRIMARY KEY(key, c) )");

        assertInvalid("select * from %s where key = 'foo' and c in (1,3,4)");
    }

    /**
     * Test for 5386,
     * migrated from cql_tests.py:TestCQL.function_and_reverse_type_test()
     */
    @Test
    public void testDescClusteringOnTimeuuid() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c timeuuid, v int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c DESC)");

        execute("INSERT INTO %s (k, c, v) VALUES (0, now(), 0)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_5404()
     */
    @Test
    public void testSelectWithToken() throws Throwable
    {
        createTable("CREATE TABLE %s (key text PRIMARY KEY)");

        // We just want to make sure this doesn 't NPE server side
        assertInvalid("select * from %s where token(key) > token(int(3030343330393233)) limit 1");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.empty_blob_test()
     */
    @Test
    public void testEmptyBlob() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, b blob)");
        execute("INSERT INTO %s (k, b) VALUES (0, 0x)");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, ByteBufferUtil.bytes("")));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.clustering_order_and_functions_test()
     */
    @Test
    public void testFunctionsWithClusteringDesc() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, t timeuuid, PRIMARY KEY (k, t) ) WITH CLUSTERING ORDER BY (t DESC)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, t) VALUES (?, now())", i);

        execute("SELECT dateOf(t) FROM %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_with_alias_test()
     */
    @Test
    public void testSelectWithAlias() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");

        for (int id = 0; id < 5; id++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 10 AND TIMESTAMP 0", id, "name" + id);

        // test aliasing count( *)
        UntypedResultSet rs = execute("SELECT count(*) AS user_count FROM %s");
        assertEquals("user_count", rs.metadata().get(0).name.toString());
        assertEquals(5L, rs.one().getLong(rs.metadata().get(0).name.toString()));

        // test aliasing regular value
        rs = execute("SELECT name AS user_name FROM %s WHERE id = 0");
        assertEquals("user_name", rs.metadata().get(0).name.toString());
        assertEquals("name0", rs.one().getString(rs.metadata().get(0).name.toString()));

        // test aliasing writetime
        rs = execute("SELECT writeTime(name) AS name_writetime FROM %s WHERE id = 0");
        assertEquals("name_writetime", rs.metadata().get(0).name.toString());
        assertEquals(0, rs.one().getInt(rs.metadata().get(0).name.toString()));

        // test aliasing ttl
        rs = execute("SELECT ttl(name) AS name_ttl FROM %s WHERE id = 0");
        assertEquals("name_ttl", rs.metadata().get(0).name.toString());
        int ttl = rs.one().getInt(rs.metadata().get(0).name.toString());
        assertTrue(ttl == 9 || ttl == 10);

        // test aliasing a regular function
        rs = execute("SELECT intAsBlob(id) AS id_blob FROM %s WHERE id = 0");
        assertEquals("id_blob", rs.metadata().get(0).name.toString());
        assertEquals(ByteBuffer.wrap(new byte[4]), rs.one().getBlob(rs.metadata().get(0).name.toString()));

        // test that select throws a meaningful exception for aliases in where clause
        assertInvalidMessage("Aliases aren't allowed in the where clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Aliases are not allowed in order by clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name");
    }

    /**
     * Test for bug #5795,
     * migrated from cql_tests.py:TestCQL.nonpure_function_collection_test()
     */
    @Test
    public void testNonPureFunctionCollection() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<timeuuid>)");

        // we just want to make sure this doesn't throw
        execute("INSERT INTO %s (k, v) VALUES (0, [now()])");
    }

    private Object[][] fill() throws Throwable
    {
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                execute("INSERT INTO %s (k1, k2, v) VALUES (?, ?, ?)", i, j, i + j);

        return getRows(execute("SELECT * FROM %s"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.empty_in_test()
     */
    @Test
    public void testEmpty() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2))");

        // Inserts a few rows to make sure we don 't actually query something
        Object[][] rows = fill();

        // Test empty IN() in SELECT
        assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
        assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));

        // Test empty IN() in DELETE
        execute("DELETE FROM %s WHERE k1 IN ()");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));

        // Test empty IN() in UPDATE
        execute("UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));

        // Same test, but for compact
        createTable("CREATE TABLE %s (k1 int, k2 int, v int, PRIMARY KEY (k1, k2)) WITH COMPACT STORAGE");

        rows = fill();

        assertEmpty(execute("SELECT v FROM %s WHERE k1 IN ()"));
        assertEmpty(execute("SELECT v FROM %s WHERE k1 = 0 AND k2 IN ()"));

        // Test empty IN() in DELETE
        execute("DELETE FROM %s WHERE k1 IN ()");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));

        // Test empty IN() in UPDATE
        execute("UPDATE %s SET v = 3 WHERE k1 IN () AND k2 = 2");
        assertArrayEquals(rows, getRows(execute("SELECT * FROM %s")));
    }

    /**
     * Test for 5805 bug,
     * migrated from cql_tests.py:TestCQL.collection_flush_test()
     */
    @Test
    public void testCollectionFlush() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<int>)");

        execute("INSERT INTO %s (k, s) VALUES (1, {1})");
        flush();

        execute("INSERT INTO %s (k, s) VALUES (1, {2})");
        flush();

        assertRows(execute("SELECT * FROM %s"),
                   row(1, set(2)));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_distinct_test()
     */
    @Test
    public void testSelectDistinct() throws Throwable
    {
        // Test a regular(CQL3) table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, ck0 int, val int, PRIMARY KEY((pk0, pk1), ck0))");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 0, 0)", i, i);
            execute("INSERT INTO %s (pk0, pk1, ck0, val) VALUES (?, ?, 1, 1)", i, i);
        }

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test selection validation.
        assertInvalidMessage("queries must request all the partition key columns", "SELECT DISTINCT pk0 FROM %s");
        assertInvalidMessage("queries must only request partition key columns", "SELECT DISTINCT pk0, pk1, ck0 FROM %s");

        //Test a 'compact storage' table.
        createTable("CREATE TABLE %s (pk0 int, pk1 int, val int, PRIMARY KEY((pk0, pk1))) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
            execute("INSERT INTO %s (pk0, pk1, val) VALUES (?, ?, ?)", i, i, i);

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 1"),
                   row(0, 0));

        assertRows(execute("SELECT DISTINCT pk0, pk1 FROM %s LIMIT 3"),
                   row(0, 0),
                   row(2, 2),
                   row(1, 1));

        // Test a 'wide row' thrift table.
        createTable("CREATE TABLE %s (pk int, name text, val int, PRIMARY KEY(pk, name)) WITH COMPACT STORAGE");

        for (int i = 0; i < 3; i++)
        {
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name0', 0)", i);
            execute("INSERT INTO %s (pk, name, val) VALUES (?, 'name1', 1)", i);
        }

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 1"),
                   row(1));

        assertRows(execute("SELECT DISTINCT pk FROM %s LIMIT 3"),
                   row(1),
                   row(0),
                   row(2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.select_distinct_with_deletions_test()
     */
    @Test
    public void testSelectDistinctWithDeletions() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c int, v int)");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, v) VALUES (?, ?, ?)", i, i, i);

        Object[][] rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(10, rows.length);
        Object key_to_delete = rows[3][0];

        execute("DELETE FROM %s WHERE k=?", key_to_delete);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s LIMIT 5"));
        Assert.assertEquals(5, rows.length);

        rows = getRows(execute("SELECT DISTINCT k FROM %s"));
        Assert.assertEquals(9, rows.length);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.function_with_null_test()
     */
    @Test
    public void testFunctionWithNull() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t timeuuid,)");

        execute("INSERT INTO %s (k) VALUES (0)");
        Object[][] rows = getRows(execute("SELECT dateOf(t) FROM %s WHERE k=0"));
        assertNull(rows[0][0]);
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6050_test()
     */
    @Test
    public void testInvalidIndexSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int)");

        createIndex("CREATE INDEX ON %s (a)");
        assertInvalid("SELECT * FROM %s WHERE a = 3 AND b IN (1, 3)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6115_test()
     */
    @Test
    public void testBatchDeleteInsert() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, v) VALUES (0, 1)");
        execute("BEGIN BATCH DELETE FROM %1$s WHERE k=0 AND v=1; INSERT INTO %1$s (k, v) VALUES (0, 2); APPLY BATCH");

        assertRows(execute("SELECT * FROM %s"),
                   row(0, 2));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.column_name_validation_test()
     */
    @Test
    public void testColumnNameValidation() throws Throwable
    {
        createTable(" CREATE TABLE %s (k text, c int, v timeuuid, PRIMARY KEY (k, c))");

        assertInvalid("INSERT INTO %s (k, c) VALUES ('', 0)");

        // Insert a value that don't fit 'int'
        assertInvalid("INSERT INTO %s (k, c) VALUES (0, 10000000000)");

        // Insert a non-version 1 uuid
        assertInvalid("INSERT INTO %s (k, c, v) VALUES (0, 0, 550e8400-e29b-41d4-a716-446655440000)");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.user_types_test()
     */
    @Test
    public void testUserTypes() throws Throwable
    {
        UUID userID_1 = UUID.fromString("550e8400-e29b-41d4-a716-446655440000");

        String addressType = createType("CREATE TYPE %s (street text, city text, zip_code int, phones set<text >)");

        String nameType = createType("CREATE TYPE %s (firstname text, lastname text)");

        createTable("CREATE TABLE %s (id uuid PRIMARY KEY, name frozen < " + nameType + " >, addresses map < text, frozen < " + addressType + " >> )");

        execute("INSERT INTO %s (id, name) VALUES(?, { firstname: 'Paul', lastname: 'smith' } )", userID_1);

        assertRows(execute("SELECT name.firstname FROM %s WHERE id = ?", userID_1), row("Paul"));

        execute("UPDATE %s SET addresses = addresses + { 'home': { street: '...', city:'SF', zip_code:94102, phones:{ } } } WHERE id = ?", userID_1);

        // TODO: deserialize the value here and check it 's right.
        execute("SELECT addresses FROM %s WHERE id = ? ", userID_1);
    }

    /**
     * Test user type test that does a little more nesting,
     * migrated from cql_tests.py:TestCQL.more_user_types_test()
     */
    @Test
    public void testNestedUserTypes() throws Throwable
    {
        String type1 = createType("CREATE TYPE %s ( s set<text>, m map<text, text>, l list<text>)");

        String type2 = createType("CREATE TYPE %s ( s set < frozen < " + type1 + " >>,)");

        createTable("CREATE TABLE %s (id int PRIMARY KEY, val frozen<" + type2 + ">)");

        execute("INSERT INTO %s (id, val) VALUES (0, { s : {{ s : {'foo', 'bar'}, m : { 'foo' : 'bar' }, l : ['foo', 'bar']} }})");

        // TODO: check result once we have an easy way to do it. For now we just check it doesn't crash
        execute("SELECT * FROM %s");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.bug_6327_test()
     */
    @Test
    public void testSelectInClauseAtOne() throws Throwable
    {
        createTable("CREATE TABLE %s ( k int, v int, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s (k, v) VALUES (0, 0)");

        flush();

        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v IN (1, 0)"),
                   row(0));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.nan_infinity_test()
     */
    @Test
    public void testNanInfinityValues() throws Throwable
    {
        createTable("CREATE TABLE %s (f float PRIMARY KEY)");

        execute("INSERT INTO %s (f) VALUES (NaN)");
        execute("INSERT INTO %s (f) VALUES (-NaN)");
        execute("INSERT INTO %s (f) VALUES (Infinity)");
        execute("INSERT INTO %s (f) VALUES (-Infinity)");

        Object[][] selected = getRows(execute("SELECT * FROM %s"));

        // selected should be[[nan],[inf],[-inf]],
        // but assert element - wise because NaN!=NaN
        assertEquals(3, selected.length);
        assertEquals(1, selected[0].length);
        assertTrue(Float.isNaN((Float) selected[0][0]));

        assertTrue(Float.isInfinite((Float) selected[1][0])); //inf
        assertTrue(((Float) selected[1][0]) > 0);

        assertTrue(Float.isInfinite((Float) selected[2][0])); //-inf
        assertTrue(((Float) selected[2][0]) < 0);
    }

    /**
     * Test for the #6579 'select count' paging bug,
     * migrated from cql_tests.py:TestCQL.select_count_paging_test()
     */
    @Test
    public void testSelectCountPaging() throws Throwable
    {
        createTable("create table %s (field1 text, field2 timeuuid, field3 boolean, primary key(field1, field2))");
        createIndex("create index test_index on %s (field3)");

        execute("insert into %s (field1, field2, field3) values ('hola', now(), false)");
        execute("insert into %s (field1, field2, field3) values ('hola', now(), false)");

        assertRows(execute("select count(*) from %s where field3 = false limit 1"),
                   row(1L));
    }

    /**
     * Test the syntax introduced by #4851,
     * migrated from cql_tests.py:TestCQL.tuple_notation_test()
     */
    @Test
    public void testTupleNotation() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, v1 int, v2 int, v3 int, PRIMARY KEY (k, v1, v2, v3))");
        for (int i = 0; i < 2; i++)
            for (int j = 0; j < 2; j++)
                for (int k = 0; k < 2; k++)
                    execute("INSERT INTO %s (k, v1, v2, v3) VALUES (0, ?, ?, ?)", i, j, k);

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0"),
                   row(0, 0, 0),
                   row(0, 0, 1),
                   row(0, 1, 0),
                   row(0, 1, 1),
                   row(1, 0, 0),
                   row(1, 0, 1),
                   row(1, 1, 0),
                   row(1, 1, 1));

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2, v3) >= (1, 0, 1)"),
                   row(1, 0, 1),
                   row(1, 1, 0),
                   row(1, 1, 1));
        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) >= (1, 1)"),
                   row(1, 1, 0),
                   row(1, 1, 1));

        assertRows(execute("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v2) > (0, 1) AND (v1, v2, v3) <= (1, 1, 0)"),
                   row(1, 0, 0),
                   row(1, 0, 1),
                   row(1, 1, 0));

        assertInvalid("SELECT v1, v2, v3 FROM %s WHERE k = 0 AND (v1, v3) > (1, 0)");
    }

    /**
     * Test for CASSANDRA-8062,
     * migrated from cql_tests.py:TestCQL.test_v2_protocol_IN_with_tuples()
     */
    @Test
    public void testSelectInStatementWithTuples() throws Throwable
    {   // TODO - the dtest was using v2 protocol
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, PRIMARY KEY (k, c1, c2))");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'a')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'b')");
        execute("INSERT INTO %s (k, c1, c2) VALUES (0, 0, 'c')");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND (c1, c2) IN ((0, 'b'), (0, 'c'))"),
                   row(0, 0, "b"),
                   row(0, 0, "c"));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.in_with_desc_order_test()
     */
    @Test
    public void testSelectInStatementWithDesc() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, PRIMARY KEY (k, c1, c2))");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 0)");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 1)");
        execute("INSERT INTO %s(k, c1, c2) VALUES (0, 0, 2)");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                   row(0, 0, 2),
                   row(0, 0, 0));
    }

    /**
     * Test that columns don't need to be selected for ORDER BY when there is a IN (#4911),
     * migrated from cql_tests.py:TestCQL.in_order_by_without_selecting_test()
     */
    @Test
    public void testInOrderByWithoutSelecting() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY (k, c1, c2))");

        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 0, 0)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 1, 1)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (0, 0, 2, 2)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 0, 3)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 1, 4)");
        execute("INSERT INTO %s (k, c1, c2, v) VALUES (1, 1, 2, 5)");

        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                   row(0, 0, 0, 0),
                   row(0, 0, 2, 2));
        assertRows(execute("SELECT * FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC, c2 ASC"),
                   row(0, 0, 0, 0),
                   row(0, 0, 2, 2));

        // check that we don 't need to select the column on which we order
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0)"),
                   row(0),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 ASC"),
                   row(0),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND c1 = 0 AND c2 IN (2, 0) ORDER BY c1 DESC"),
                   row(2),
                   row(0));
        assertRows(execute("SELECT v FROM %s WHERE k IN (1, 0)"),
                   row(3),
                   row(4),
                   row(5),
                   row(0),
                   row(1),
                   row(2));

        assertRows(execute("SELECT v FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"),
                   row(0),
                   row(1),
                   row(2),
                   row(3),
                   row(4),
                   row(5));

        // we should also be able to use functions in the select clause (additional test for CASSANDRA - 8286)
        Object[][] results = getRows(execute("SELECT writetime(v) FROM %s WHERE k IN (1, 0) ORDER BY c1 ASC"));

        // since we don 't know the write times, just assert that the order matches the order we expect
        assertTrue(isFirstIntSorted(results));
    }

    private boolean isFirstIntSorted(Object[][] rows)
    {
        for (int i = 1; i < rows.length; i++)
        {
            Long prev = (Long)rows[i-1][0];
            Long curr = (Long)rows[i][0];

            if (prev > curr)
                return false;
        }

        return true;
    }

    /**
     * Check for #7052 bug,
     * migrated from cql_tests.py:TestCQL.limit_compact_table()
     */
    @Test
    public void testLimitInCompactTable() throws Throwable
    {
        createTable("CREATE TABLE %s( k int, v int, PRIMARY KEY (k, v) ) WITH COMPACT STORAGE ");

        for (int i = 0; i < 4; i++)
            for (int j = 0; j < 4; j++)
                execute("INSERT INTO %s(k, v) VALUES (?, ?)", i, j);

        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > 0 AND v <= 4 LIMIT 2"),
                   row(1),
                   row(2));
        assertRows(execute("SELECT v FROM %s WHERE k=0 AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0),
                   row(1));

        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 2"),
                   row(0, 1),
                   row(0, 2));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > -1 AND v <= 4 LIMIT 2"),
                   row(0, 0),
                   row(0, 1));
        assertRows(execute("SELECT * FROM %s WHERE k IN (0, 1, 2) AND v > 0 AND v <= 4 LIMIT 6"),
                   row(0, 1),
                   row(0, 2),
                   row(0, 3),
                   row(1, 1),
                   row(1, 2),
                   row(1, 3));

        // This doesn't work -- see #7059
        //assertRows(execute("SELECT * FROM %s WHERE v > 1 AND v <= 3 LIMIT 6 ALLOW FILTERING"),
        //           row(1, 2),
        //           row(1, 3),
        //           row(0, 2),
        //           row(0, 3),
        //           row(2, 2),
        //           row(2, 3));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.invalid_custom_timestamp_test()
     */
    @Test
    public void testInvalidCustomTimestamp() throws Throwable
    {
        // Conditional updates
        createTable("CREATE TABLE %s (k int, v int, PRIMARY KEY (k, v))");

        execute("BEGIN BATCH " +
                "INSERT INTO %1$s (k, v) VALUES(0, 0) IF NOT EXISTS; " +
                "INSERT INTO %1$s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                "APPLY BATCH");

        assertInvalid("BEGIN BATCH " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 2) IF NOT EXISTS USING TIMESTAMP 1; " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 3) IF NOT EXISTS; " +
                      "APPLY BATCH");
        assertInvalid("BEGIN BATCH " +
                      "USING TIMESTAMP 1 INSERT INTO %1$s (k, v) VALUES(0, 4) IF NOT EXISTS; " +
                      "INSERT INTO %1$s (k, v) VALUES(0, 1) IF NOT EXISTS; " +
                      "APPLY BATCH");

        execute("INSERT INTO %s (k, v) VALUES(1, 0) IF NOT EXISTS");
        assertInvalid("INSERT INTO %s (k, v) VALUES(1, 1) IF NOT EXISTS USING TIMESTAMP 5");

        // Counters
        createTable("CREATE TABLE %s (k int PRIMARY KEY, c counter)");

        execute("UPDATE %s SET c = c + 1 WHERE k = 0");
        assertInvalid("UPDATE %s USING TIMESTAMP 10 SET c = c + 1 WHERE k = 0");

        execute("BEGIN COUNTER BATCH " +
                "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                "APPLY BATCH");

        assertInvalid("BEGIN COUNTER BATCH " +
                      "UPDATE %1$s USING TIMESTAMP 3 SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH");

        assertInvalid("BEGIN COUNTER BATCH " +
                      "USING TIMESTAMP 3 UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "UPDATE %1$s SET c = c + 1 WHERE k = 0; " +
                      "APPLY BATCH");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.add_field_to_udt_test()
     */
    @Test
    public void testAddFieldToUdt() throws Throwable
    {
        String typeName = createType("CREATE TYPE %s (fooint int, fooset set <text>)");
        createTable("CREATE TABLE %s (key int PRIMARY KEY, data frozen <" + typeName + ">)");

        execute("INSERT INTO %s (key, data) VALUES (1, {fooint: 1, fooset: {'2'}})");
        execute("ALTER TYPE " + keyspace() + "." + typeName + " ADD foomap map <int,text>");
        execute("INSERT INTO %s (key, data) VALUES (1, {fooint: 1, fooset: {'2'}, foomap: {3 : 'bar'}})");
    }

    /**
     * Test for #7105 bug,
     * migrated from cql_tests.py:TestCQL.clustering_order_in_test()
     */
    @Test
    public void testClusteringOrder() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY ((a, b), c) ) with clustering order by (c desc)");

        execute("INSERT INTO %s (a, b, c) VALUES (1, 2, 3)");
        execute("INSERT INTO %s (a, b, c) VALUES (4, 5, 6)");

        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3)"),
                   row(1, 2, 3));
        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 AND c IN (3, 4)"),
                   row(1, 2, 3));
    }

    /**
     * Test for #7105 bug,
     * SELECT with IN on final column of composite and compound primary key fails
     * migrated from cql_tests.py:TestCQL.bug7105_test()
     */
    @Test
    public void testSelectInFinalColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c int, d int, PRIMARY KEY (a, b))");

        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 2, 3, 3)");
        execute("INSERT INTO %s (a, b, c, d) VALUES (1, 4, 6, 5)");

        assertRows(execute("SELECT * FROM %s WHERE a=1 AND b=2 ORDER BY b DESC"),
                   row(1, 2, 3, 3));
    }

    /**
     * Migrated from cql_tests.py:TestCQL.blobAs_functions_test()
     */
    @Test
    public void testBlobAsFunction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        //  A blob that is not 4 bytes should be rejected
        assertInvalid("INSERT INTO %s (k, v) VALUES (0, blobAsInt(0x01))");

        execute("INSERT INTO %s (k, v) VALUES (0, blobAsInt(0x00000001))");
        assertRows(execute("select v from %s where k=0"), row(1));
    }

    /**
     * Test for 6276,
     * migrated from cql_tests.py:TestCQL.drop_and_readd_collection_test()
     */
    @Test
    public void testDropAndReaddCollection() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>, x int)");
        execute("insert into %s (k, v) VALUES (0, {'fffffffff'})");
        flush();
        execute("alter table %s drop v");
        assertInvalid("alter table %s add v set<int>");
    }

    /**
     * Test for 7744,
     * migrated from cql_tests.py:TestCQL.downgrade_to_compact_bug_test()
     */
    @Test
    public void testDowngradeToCompact() throws Throwable
    {
        createTable("create table %s (k int primary key, v set<text>)");
        execute("insert into %s (k, v) VALUES (0, {'f'})");
        flush();
        execute("alter table %s drop v");
        execute("alter table %s add v int");
    }

    /**
     * Migrated from cql_tests.py:TestCQL.negative_timestamp_test()
     */
    @Test
    public void testNegativeTimestamp() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");
        execute("INSERT INTO %s (k, v) VALUES (1, 1) USING TIMESTAMP -42");

        assertRows(execute("SELECT writetime(v) FROM %s WHERE k = 1"), row(-42L));
    }

    /**
     * Test for CASSANDRA-8558, deleted row still can be selected out
     * migrated from cql_tests.py:TestCQL.bug_8558_test()
     */
    @Test
    public void testDeletedRowCannotBeSelected() throws Throwable
    {
        createTable("CREATE TABLE %s (a int, b int, c text,primary key(a,b))");
        execute("INSERT INTO %s (a,b,c) VALUES(1,1,'1')");
        flush();

        execute("DELETE FROM %s  where a=1 and b=1");
        flush();

        assertEmpty(execute("select * from %s  where a=1 and b=1"));
    }
}