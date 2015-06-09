package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

public class LimitTest extends CQLTester
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.setPartitioner(new ByteOrderedPartitioner());
    }

    /**
     * Test limit across a partition range, requires byte ordered partitioner,
     * migrated from cql_tests.py:TestCQL.limit_range_test()
     */
    @Test
    public void testPartitionRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        assertRows(execute("SELECT * FROM %s WHERE token(userid) >= token(2) LIMIT 1"),
                   row(2, "http://foo.com", 42L));

        assertRows(execute("SELECT * FROM %s WHERE token(userid) > token(2) LIMIT 1"),
                   row(3, "http://foo.com", 42L));
    }

    /**
     * Test limit across a column range,
     * migrated from cql_tests.py:TestCQL.limit_multiget_test()
     */
    @Test
    public void testColumnRange() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, time bigint, PRIMARY KEY (userid, url)) WITH COMPACT STORAGE");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, time) VALUES (?, ?, ?)", i, String.format("http://foo.%s", tld), 42L);

        // Check that we do limit the output to 1 *and* that we respect query
        // order of keys (even though 48 is after 2)
        assertRows(execute("SELECT * FROM %s WHERE userid IN (48, 2) LIMIT 1"),
                   row(48, "http://foo.com", 42L));
    }

    /**
     * Test limit queries on a sparse table,
     * migrated from cql_tests.py:TestCQL.limit_sparse_test()
     */
    @Test
    public void testSparseTable() throws Throwable
    {
        createTable("CREATE TABLE %s (userid int, url text, day int, month text, year int, PRIMARY KEY (userid, url))");

        for (int i = 0; i < 100; i++)
            for (String tld : new String[] { "com", "org", "net" })
                execute("INSERT INTO %s (userid, url, day, month, year) VALUES (?, ?, 1, 'jan', 2012)", i, String.format("http://foo.%s", tld));

        assertRowCount(execute("SELECT * FROM %s LIMIT 4"), 4);

    }
}
