package org.apache.cassandra.db.index;

import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
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
    
    //These columns have already been created with an index by SchemaLoader
    private final static String KS1_INDEXED_COL_NAME = "birthdate";
    private final static String KS2_INDEXED_COL_NAME = "col1";
    
    //These columns should be created first
    private final static String KS1_INDEXED_MANUAL_COL_NAME = "state";
    private final static String KS2_INDEXED_MANUAL_COL_NAME = "col2";
    
    private final static String KS1_NON_INDEXED_COL_NAME = "userid";
    private final static String KS2_NON_INDEXED_COL_NAME = "col3";
    
    private final int MAX_RESULTS = 100;
    
    private void testIndexScanSimple(String keySpaceName, 
            String columnFamilyName, 
            Map<String, Map<String, Long>> values, 
            IndexExpression expr,
            Map<String, Map<String, Long>> expectedValues)
    {
        testIndexScanSimple(keySpaceName, 
                            columnFamilyName, 
                            values,
                            Arrays.asList(expr), 
                            expectedValues);
    }
    
    private void testIndexScanSimple(String keySpaceName, 
                                 String columnFamilyName, 
                                 Map<String, Map<String, Long>> values, 
                                 List<IndexExpression> clause,
                                 Map<String, Map<String, Long>> expectedValues) 
    {
        ColumnFamilyStore cfs = Keyspace.open(keySpaceName).getColumnFamilyStore(columnFamilyName);
        
        for (Map.Entry<String, Map<String, Long>> entry : values.entrySet())
        {
            Mutation rm = new Mutation(keySpaceName, ByteBufferUtil.bytes(entry.getKey()));
            
            for (Map.Entry<String, Long> cell : entry.getValue().entrySet()) 
            {
                CellName cellName = cellname(cell.getKey());
                rm.add(columnFamilyName, cellName, ByteBufferUtil.bytes(cell.getValue()), 0);
                rm.apply();
            }
        }
        
        List<Row> rows = search(clause, cfs);

        assertNotNull(rows);
        assertEquals(expectedValues.size(), rows.size());

        for (Row row : rows)
        {
            String key = new String(row.key.getKey().array(), row.key.getKey().position(), row.key.getKey().remaining());
            
            assertTrue(expectedValues.containsKey(key));
            for (Map.Entry<String, Long> cell : expectedValues.get(key).entrySet()) 
            {
                CellName cellName = cellname(cell.getKey());
                assertEquals(ByteBufferUtil.bytes(cell.getValue()), row.cf.getColumn(cellName).value());
            }
        }
    }
    
    private void testIndexScanComposite(String keySpaceName, 
            String columnFamilyName, 
            Map<String, Map<String, Long>> values, 
            IndexExpression expr,
            Map<String, Map<String, Long>> expectedValues)
    {
        testIndexScanComposite(keySpaceName, 
                columnFamilyName, 
                values,
                Arrays.asList(expr), 
                expectedValues);
    }
    
    private void testIndexScanComposite(String keySpaceName, 
            String columnFamilyName, 
            Map<String, Map<String, Long>> values, 
            List<IndexExpression> clause,
            Map<String, Map<String, Long>> expectedValues) {

        Keyspace keyspace = Keyspace.open(keySpaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);
        
        CellNameType baseComparator = cfs.getComparator();
        
        for (Map.Entry<String, Map<String, Long>> entry : values.entrySet())
        {
            Mutation rm = new Mutation(keySpaceName, ByteBufferUtil.bytes(entry.getKey()));

            for (Map.Entry<String, Long> cell : entry.getValue().entrySet()) 
            {
                ByteBuffer colName = ByteBufferUtil.bytes(cell.getKey());
                ByteBuffer clusterKey = ByteBufferUtil.bytes("c" + entry.getKey());

                CellName compositeName = baseComparator.makeCellName(clusterKey, colName);
                ByteBuffer val1 = ByteBufferUtil.bytes(cell.getValue());

                rm.add(columnFamilyName, compositeName, val1, 0);
                rm.apply();
            }
        }
        
        List<Row> rows = search(clause, cfs);

        assertNotNull(rows);
        assertEquals(expectedValues.size(), rows.size());

        for (Row row : rows)
        {
            String key = new String(row.key.getKey().array(), row.key.getKey().position(), row.key.getKey().remaining());
            ByteBuffer clusterKey = ByteBufferUtil.bytes("c" + key);
            assertTrue(expectedValues.containsKey(key));
            
            for (Map.Entry<String, Long> cell : expectedValues.get(key).entrySet()) 
            {
                CellName compositeName = baseComparator.makeCellName(clusterKey, cell.getKey());
                assertEquals(ByteBufferUtil.bytes(cell.getValue()), row.cf.getColumn(compositeName).value());
            }
        }
    }

    private List<Row> search(List<IndexExpression> clause, ColumnFamilyStore cfs)
    {
        List<Row> rows = null;
        if (!cfs.indexManager.hasIndexFor(clause))
            return rows;
        
        Range<RowPosition> range = Util.range("", "");
        ExtendedFilter filter = cfs.makeExtendedFilter(range, 
                                                       new IdentityQueryFilter(), 
                                                       clause, 
                                                       MAX_RESULTS, 
                                                       false, 
                                                       false, 
                                                       System.currentTimeMillis());
        
        assertNotNull(filter);
        
        SecondaryIndexSearcher mostSelective = cfs.indexManager.mostSelectiveIndexSearcher(filter);
        assertNotNull(mostSelective);
        
        IndexExpression mostSelectiveExpr = mostSelective.highestSelectivityPredicate(filter.getClause());
        assertNotNull(mostSelectiveExpr);
        
        assertEquals(clause.get(0), mostSelectiveExpr); 
        
        rows = mostSelective.search(filter);
        return rows;
    }
    
    private void addColumn(String keySpaceName, 
                               String columnFamilyName, 
                               String columnName, 
                               IndexType indexType) throws InterruptedException, ExecutionException, ConfigurationException 
    {
        Keyspace keyspace = Keyspace.open(keySpaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);
        
        ByteBuffer colName = ByteBufferUtil.bytes(columnName);
        
        ColumnDefinition cd = ColumnDefinition.regularDef(cfs.metadata, colName, UTF8Type.instance, 1);
        assertNotNull(cd);
        
        cfs.metadata.addColumnDefinition(cd);
        
        if (indexType != null)
        {
            cd.setIndex(columnName + "_index", indexType, Collections.<String, String>emptyMap());
            Future<?> future = cfs.indexManager.addIndexedColumn(cd);
            future.get();
        }
    }
    
    //Each key will contain the same column with the given values one per key
    Map<String, Map<String,Long>> makeMapForColumn(String columnName, String [] keys, Long[] values)
    {
        Map<String, Map<String,Long>> ret = new LinkedHashMap<String, Map<String,Long>>();
        addKeysForColumn(ret, columnName, keys, values);
        return ret;
    }
    
    void addKeysForColumn(Map<String, Map<String,Long>> map, String columnName, String [] keys, Long[] values)
    {
        int v = 0;
        for (String key : keys) 
        {
            Map<String,Long> keyMap = map.get(key);
            if (keyMap == null)
            {
                keyMap = new LinkedHashMap<String, Long>();
                map.put(key, keyMap);
            }
            keyMap.put(columnName, values[v++]);
        }
    }
    
    @Test
    public void testIndexScan_Simple_EQ()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk2", "kk4"}, 
                                                                        new Long[] {2L, 2L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_EQ()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});

        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk2", "kk4"}, 
                                                                        new Long[] {2L, 2L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, expr, expectedValues);
    }
    
    // TestsFor CASSANDRA-4476 (Support 2ndary index queries with only non-EQ clauses)
    
    @Test
    public void testIndexScan_Simple_GTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});

        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk2", "kk4", "kk3"}, 
                                                                        new Long[] {2L, 2L, 3L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(2L));
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_GT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk3"}, 
                                                                        new Long[] {3L});

        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_LTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});

        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk1", "kk2", "kk4"}, 
                                                                        new Long[] {1L, 2L, 2L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(2L));
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_LT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk1"}, 
                                                                        new Long[] {1L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LT, ByteBufferUtil.bytes(2L));
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_EQ_GT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 4L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk4"}, 
                                                                        new Long[] {4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(4L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(1L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Simple_GT_LTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 4L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS1_INDEXED_COL_NAME, 
                                                                        new String[] {"kk3", "kk4"}, 
                                                                        new Long[] {3L, 4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(4L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanSimple(KS1, KS1_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_GTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});

        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk2", "kk4", "kk3"}, 
                                                                        new Long[] {2L, 2L, 3L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(2L));
        
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_GT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk3"}, 
                                                                        new Long[] {3L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_LTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});

        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk1", "kk2", "kk4"}, 
                                                                        new Long[] {1L, 2L, 2L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(2L));
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_LT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk1"}, 
                                                                        new Long[] {1L});
        
        IndexExpression expr = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.LT, ByteBufferUtil.bytes(2L));
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, expr, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_EQ_GT()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 4L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk4"}, 
                                                                        new Long[] {4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(4L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(1L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_GT_LTE()
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 4L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] {"kk3", "kk4"}, 
                                                                        new Long[] {3L, 4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.GT, ByteBufferUtil.bytes(2L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(4L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_TwoColumns() throws InterruptedException, ExecutionException, ConfigurationException 
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] { "kk4"}, 
                                                                        new Long[] {4L});
        
        addColumn(KS2, KS2_CF_NAME, KS2_NON_INDEXED_COL_NAME, null);
        
        addKeysForColumn(values, KS2_NON_INDEXED_COL_NAME, new String[] {"kk3", "kk4"}, new Long[] {3L, 4L});
        addKeysForColumn(expectedValues, KS2_NON_INDEXED_COL_NAME, new String[] {"kk4"}, new Long[] {4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS2_NON_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(4L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Composite_TwoIndexedColumns() throws InterruptedException, ExecutionException, ConfigurationException 
    {
        Map<String, Map<String,Long>> values = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                new String[] {"kk1", "kk2", "kk3", "kk4"}, 
                                                                new Long[] {1L, 2L, 3L, 2L});
        
        Map<String, Map<String,Long>> expectedValues = makeMapForColumn(KS2_INDEXED_COL_NAME, 
                                                                        new String[] { "kk4"}, 
                                                                        new Long[] {4L});
        
        addColumn(KS2, KS2_CF_NAME, KS2_INDEXED_MANUAL_COL_NAME, IndexType.COMPOSITES);
        
        addKeysForColumn(values, KS2_INDEXED_MANUAL_COL_NAME, new String[] {"kk3", "kk4"}, new Long[] {3L, 4L});
        addKeysForColumn(expectedValues, KS2_INDEXED_MANUAL_COL_NAME, new String[] {"kk4"}, new Long[] {4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_MANUAL_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(4L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, clause, expectedValues);
    }

}
