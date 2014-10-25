package org.apache.cassandra.db.index;

import static org.apache.cassandra.Util.cellname;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.columniterator.IdentityQueryFilter;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNameType;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.BeforeClass;
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
    
    // according to Junit doc this should run after SchemaLoader.loadSchema()
    // http://junit.sourceforge.net/javadoc/org/junit/BeforeClass.html
    @BeforeClass
    public static void enhanceSchema() throws ConfigurationException, InterruptedException, ExecutionException 
    {
        addColumn(KS1, KS1_CF_NAME, KS1_INDEXED_MANUAL_COL_NAME, IndexType.KEYS);
        addColumn(KS2, KS2_CF_NAME, KS2_INDEXED_MANUAL_COL_NAME, IndexType.COMPOSITES);
        
        addColumn(KS1, KS1_CF_NAME, KS1_NON_INDEXED_COL_NAME, null);
        addColumn(KS2, KS2_CF_NAME, KS2_NON_INDEXED_COL_NAME, null);
    }
    
    private static void addColumn(String keySpaceName, 
            String columnFamilyName, 
            String columnName, 
            IndexType indexType) throws InterruptedException, ExecutionException, ConfigurationException 
    {
        Keyspace keyspace = Keyspace.open(keySpaceName);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);

        ByteBuffer colName = ByteBufferUtil.bytes(columnName);

        if (indexType == null)
        {
            ColumnDefinition cd = ColumnDefinition.regularDef(cfs.metadata, colName, UTF8Type.instance, 0);
            cfs.metadata.addColumnDefinition(cd);
        }
        else
        {
            ColumnDefinition cd = null;
            if (indexType == IndexType.COMPOSITES)
            {
                cd = ColumnDefinition.regularDef(cfs.metadata, colName, UTF8Type.instance, 1);
                cfs.metadata.addColumnDefinition(cd);

                cd.setIndex(columnName + "_index", indexType, Collections.<String, String>emptyMap());
            }
            else if (indexType == IndexType.KEYS)
            {
                cd = ColumnDefinition.regularDef(cfs.metadata, colName, LongType.instance, null);
                cfs.metadata.addColumnDefinition(cd);

                cd.setIndex(columnName + "_index", indexType, null);
            }
            else
            {
                throw new RuntimeException("Unsupported index type");
            }

            Future<?> future = cfs.indexManager.addIndexedColumn(cd);
            future.get();
        }
    }
    
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
        
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Map<String, Long>> entry : values.entrySet())
        {
            Mutation rm = new Mutation(keySpaceName, ByteBufferUtil.bytes(entry.getKey()));
            rm.delete(columnFamilyName, now);
            
            for (Map.Entry<String, Long> cell : entry.getValue().entrySet()) 
            {
                CellName cellName = cellname(cell.getKey());
                rm.add(columnFamilyName, cellName, ByteBufferUtil.bytes(cell.getValue()), now+1);
            }
            
            rm.apply();
        }
        
        List<Row> rows = search(clause, cfs);
        verifyRows(rows, expectedValues.size());

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

        ColumnFamilyStore cfs = Keyspace.open(keySpaceName).getColumnFamilyStore(columnFamilyName);
        CellNameType baseComparator = cfs.getComparator();
        
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Map<String, Long>> entry : values.entrySet())
        {
            Mutation rm = new Mutation(keySpaceName, ByteBufferUtil.bytes(entry.getKey()));
            rm.delete(columnFamilyName, now);
            
            for (Map.Entry<String, Long> cell : entry.getValue().entrySet()) 
            {
                ByteBuffer colName = ByteBufferUtil.bytes(cell.getKey());
                ByteBuffer clusterKey = ByteBufferUtil.bytes("c" + entry.getKey());

                CellName compositeName = baseComparator.makeCellName(clusterKey, colName);
                ByteBuffer val1 = ByteBufferUtil.bytes(cell.getValue());

                rm.add(columnFamilyName, compositeName, val1, now + 1);
            }
            
            rm.apply();
        }
        
        List<Row> rows = search(clause, cfs);
        verifyRows(rows, expectedValues.size());

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
        if (!cfs.indexManager.hasIndexFor(clause))
            return null;
        
        return cfs.search(Util.range("", ""), clause, new IdentityQueryFilter(), MAX_RESULTS);
    }
    
    //Each key will contain the same column with the given values one per key
    private Map<String, Map<String,Long>> makeMapForColumn(String columnName, String [] keys, Long[] values)
    {
        Map<String, Map<String,Long>> ret = new LinkedHashMap<String, Map<String,Long>>();
        addKeysForColumn(ret, columnName, keys, values);
        return ret;
    }
    
    private void addKeysForColumn(Map<String, Map<String,Long>> map, String columnName, String [] keys, Long[] values)
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
    
    private void verifyRows(List<Row> rows, int expectedNum) 
    {
        assertNotNull(rows);
        assertEquals(expectedNum, rows.size());
        
        //make sure no duplicates
        Set<DecoratedKey> keys = new HashSet<DecoratedKey>();
        for (Row row : rows)
            keys.add(row.key);

        assertEquals(rows.size(), keys.size());
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
                                                                        new Long[] {2L});
        
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
                                                                        new Long[] {2L});
        
        addKeysForColumn(values, KS2_INDEXED_MANUAL_COL_NAME, new String[] {"kk3", "kk4"}, new Long[] {3L, 4L});
        addKeysForColumn(expectedValues, KS2_INDEXED_MANUAL_COL_NAME, new String[] {"kk4"}, new Long[] {4L});
        
        IndexExpression expr1 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(2L));
        IndexExpression expr2 = new IndexExpression(ByteBufferUtil.bytes(KS2_INDEXED_MANUAL_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(4L));
        
        List<IndexExpression> clause = Arrays.asList(expr1, expr2);
        
        testIndexScanComposite(KS2, KS2_CF_NAME, values, clause, expectedValues);
    }
    
    @Test
    public void testIndexScan_Keys_TwoColumns_Large()
    {
        Mutation rm;
        ColumnFamilyStore cfs = Keyspace.open(KS1).getColumnFamilyStore(KS1_CF_NAME);
        for (int i = 0; i < 100; i++)
        {
            rm = new Mutation(KS1, ByteBufferUtil.bytes("key" + i));
            rm.add(KS1_CF_NAME, cellname(KS1_INDEXED_COL_NAME), ByteBufferUtil.bytes(34L), 0);
            rm.add(KS1_CF_NAME, cellname(KS1_NON_INDEXED_COL_NAME), ByteBufferUtil.bytes((long) (i % 2)), 0);
            rm.applyUnsafe();
        }

        IndexExpression idxExprEQ = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(34L));
        IndexExpression idxExprLTE = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(34L));
        IndexExpression idxExprGTE = new IndexExpression(ByteBufferUtil.bytes(KS1_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(34L));
        
        IndexExpression auxExprEQ = new IndexExpression(ByteBufferUtil.bytes(KS1_NON_INDEXED_COL_NAME), IndexExpression.Operator.EQ, ByteBufferUtil.bytes(1L));
        IndexExpression auxExprLTE = new IndexExpression(ByteBufferUtil.bytes(KS1_NON_INDEXED_COL_NAME), IndexExpression.Operator.LTE, ByteBufferUtil.bytes(1L));
        IndexExpression auxExprGTE = new IndexExpression(ByteBufferUtil.bytes(KS1_NON_INDEXED_COL_NAME), IndexExpression.Operator.GTE, ByteBufferUtil.bytes(1L));
        
        List<IndexExpression> clause = Arrays.asList(idxExprEQ, auxExprEQ);
        IDiskAtomFilter filter = new IdentityQueryFilter();
        Range<RowPosition> range = Util.range("", "");
        
        //check EQ + EQ
        List<Row> rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 50);

        //check EQ + EQ smaller limit
        rows = cfs.search(range, clause, filter, 10);
        verifyRows(rows, 10);
        
        //check EQ + EQ higher limit
        rows = cfs.search(range, clause, filter, 10000);
        verifyRows(rows, 50);
        
        //check LTE + EQ
        clause = Arrays.asList(idxExprLTE, auxExprEQ);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 50);
        
        //check LTE + LTE
        clause = Arrays.asList(idxExprLTE, auxExprLTE);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 100);
        
        //check LTE + GTE
        clause = Arrays.asList(idxExprLTE, auxExprGTE);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 50);
        
        //check GTE + EQ
        clause = Arrays.asList(idxExprGTE, auxExprEQ);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 50);
        
        //check GTE + LTE
        clause = Arrays.asList(idxExprGTE, auxExprLTE);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 100);
        
        //check GTE + GTE
        clause = Arrays.asList(idxExprGTE, auxExprGTE);
        rows = cfs.search(range, clause, filter, 100);
        verifyRows(rows, 50);
    }
   

}
