package org.apache.cassandra.db.index;

import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Test;

public class SecondaryIndexSearcherTest extends SchemaLoader
{
    //The names of the keyspaces we will be using, they are defined and loaded by SchemaLoader
    private final static String KS1 = "Keyspace1";
    private final static String KS2 = "Keyspace2";
    
    //The names of the column families that we will be using, one per key space
    private final static String KS1_CF_NAME = "Indexed1";
    private final static String KS2_CF_NAME = "Indexed2";
    
    //The names of the columns that have an index, one per cf, refer to SchemaLoader
    private final static String KS1_INDEXED_COL_NAME = "birthdate";
    private final static String KS2_INDEXED_COL_NAME = "col1";
    
    private void testIndexScanSimple(String keySpaceName, 
                                 String columnFamilyName, 
                                 String columnName,
                                 Map<String, Long> values, 
                                 IndexExpression expr,
                                 Map<String, Long> expectedValues) 
    {
        ColumnFamilyStore cfs = Keyspace.open(keySpaceName).getColumnFamilyStore(columnFamilyName);
        CellName cellName = cellname(columnName);
  
        for (Map.Entry<String, Long> entry : values.entrySet())
        {
            Mutation rm = new Mutation(keySpaceName, ByteBufferUtil.bytes(entry.getKey()));
            rm.add(columnFamilyName, cellName, ByteBufferUtil.bytes(entry.getValue()), 0);
            rm.apply();
        }
        
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");

        List<Row> rows = null;
        if (cfs.indexManager.hasIndexFor(clause)) 
        {
            rows = cfs.search(range, clause, filter, 100);
        }

        assertNotNull(rows);
        assertEquals(expectedValues.size(), rows.size());

        for (Row row : rows)
        {
            String key = new String(row.key.getKey().array(), row.key.getKey().position(), row.key.getKey().remaining());
            
            assertTrue(expectedValues.containsKey(key));
            assertEquals(ByteBufferUtil.bytes(expectedValues.get(key)), row.cf.getColumn(cellName).value());
        }
    }
    
    
    private void testIndexScanComposite(String keySpace, 
            String cfName, 
            String columnName,
            Map<String, Long> values, 
            IndexExpression expr,
            Map<String, Long> expectedValues) {

        Keyspace keyspace = Keyspace.open(keySpace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(cfName);
        
        CellNameType baseComparator = cfs.getComparator();
        ByteBuffer colName = ByteBufferUtil.bytes(columnName);
        
        //cfs.truncateBlocking();

        for (Map.Entry<String, Long> entry : values.entrySet())
        {
            ByteBuffer rowKey = ByteBufferUtil.bytes(entry.getKey());
            ByteBuffer clusterKey = ByteBufferUtil.bytes("c" + entry.getKey());

            CellName compositeName = baseComparator.makeCellName(clusterKey, colName);
            ByteBuffer val1 = ByteBufferUtil.bytes(entry.getValue());

            Mutation rm = new Mutation(keySpace, rowKey);
            rm.add(cfName, compositeName, val1, 0);
            rm.apply();
        }
        
        List<IndexExpression> clause = Arrays.asList(expr);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
    
        List<Row> rows = null;
        if (cfs.indexManager.hasIndexFor(clause)) 
        {
            rows = keyspace.getColumnFamilyStore(cfName).search(range, clause, filter, 100);
        }

        assertNotNull(rows);
        assertEquals(expectedValues.size(), rows.size());

        for (Row row : rows)
        {
            String key = new String(row.key.getKey().array(), row.key.getKey().position(), row.key.getKey().remaining());
            ByteBuffer clusterKey = ByteBufferUtil.bytes("c" + key);
            
            assertTrue(expectedValues.containsKey(key));
            
            CellName compositeName = baseComparator.makeCellName(clusterKey, colName);
            assertEquals(ByteBufferUtil.bytes(expectedValues.get(key)), row.cf.getColumn(compositeName).value());
        }
        
    }
    @Test
    public void testIndexScan_Simple_EQ()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, KS1_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_EQ()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, KS2_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    // TestsFor CASSANDRA-4476 (Support 2ndary index queries with only non-EQ clauses)
    
    @Test
    public void testIndexScan_Simple_GTE()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        expectedValues.put("kk3", 3L);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, KS1_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_GT()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk3", 3L);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, KS1_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_LTE()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk1", 1L);
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, KS1_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_LT()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LT, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk1", 1L);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, KS1_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_GTE()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        expectedValues.put("kk3", 3L);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, KS2_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_GT()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk3", 3L);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, KS2_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_LTE()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk1", 1L);
        expectedValues.put("kk2", 2L);
        expectedValues.put("kk4", 2L);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, KS2_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_LT()
    {
        Map<String, Long> values = new LinkedHashMap<String, Long>();
        Map<String, Long> expectedValues = new LinkedHashMap<String, Long>();
        
        values.put("kk1", 1L);
        values.put("kk2", 2L);
        values.put("kk3", 3L);
        values.put("kk4", 2L);
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.LT, ByteBufferUtil.bytes(2L));
        
        expectedValues.put("kk1", 1L);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, KS2_INDEXED_COL_NAME, values, expr, expectedValues);
    }
    

}
