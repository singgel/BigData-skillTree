package com.hks.springhbase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * @Author: hekuangsheng
 * @Date: 2018/11/1
 */
public class HbaseUtil {

    private static Connection conn;
    //zookeeper集群
    private static String zookeeper = "192.168.70.3:2181,192.168.70.4:2181,192.168.87.234:2181";

    public static void init() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeper);
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * <建表>
     *
     * @param tableName
     * @param familyStr
     *            void
     * @throws
     */
    public static void createTable(String tableName, String familyStr) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] family = familyStr.split(",");
                for (String s : family) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    /**
     *
     * <添加列族>
     *
     * @param tableName
     * @param family
     *            void
     * @throws
     */
    public static void addFamily(String tableName, String family) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            HColumnDescriptor columnDesc = new HColumnDescriptor(family);
            admin.addColumn(table, columnDesc);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    /**
     *
     * <添加数据>
     *
     * @param tableName
     * @param family
     * @param rowKey
     * @param columns
     * @throws IOException
     *             void
     * @throws
     */
    public static void addData(String tableName, String family, String rowKey,
                               Map<String, Object> columns) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String col : columns.keySet()) {
                Object val = columns.get(col);
                if (null != val) {
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col),
                            objectToByte(val));
                }
            }
            table.put(put);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    /**
     *
     * <根据rowkey获取数据>
     *
     * @param tableName
     * @param family
     * @param rowKey
     * @return Map<String,String>
     * @throws
     */
    public static Map<String, String> getValue(String tableName, String family,
                                               String rowKey) {
        Table table = null;
        Map<String, String> resultMap = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(family.getBytes());
            Result res = table.get(get);
            Map<byte[], byte[]> result = res.getFamilyMap(family.getBytes());
            Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();
            resultMap = new HashMap<String, String>();
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                resultMap.put(Bytes.toString(entry.getKey()),
                        (String) restore(entry.getValue()));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultMap;
    }

    /**
     *
     * <根据rowkey和column获取数据>
     *
     * @param tableName
     * @param family
     * @param rowKey
     * @param column
     * @return String
     * @throws
     */
    public static String getValue(String tableName, String family,
                                  String rowKey, String column) {
        Table table = null;
        String resultStr = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
            Result res = table.get(get);
            byte[] result = res.getValue(Bytes.toBytes(family),
                    Bytes.toBytes(column));
            resultStr = (String) restore(result);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultStr;

    }

    /**
     *
     * <查询所有数据>
     *
     * @param tableName
     *            void
     * @throws
     */
    public static void getValue(String tableName) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("获得到rowkey:" + Bytes.toString(r.getRow()));
                for (Cell cell : r.rawCells()) {
                    System.out.println("列："
                            + Bytes.toString(CellUtil.cloneFamily(cell))
                            + "====值:"
                            + (String) restore(CellUtil.cloneValue(cell)));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    /**
     *
     * <删除指定名称的列簇>
     *
     * @param tableName
     * @param family
     *            void
     * @throws
     */
    public static void deleteFamily(String tableName, String family) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            admin.deleteColumn(table, Bytes.toBytes(family));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    /**
     *
     * <删除行>
     *
     * @param tableName
     * @param rowKey
     *            void
     * @throws
     */
    public static void deleteRow(String tableName, String rowKey) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            table.delete(new Delete(Bytes.toBytes(rowKey)));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(table);

        }
    }

    /**
     *
     * <删除表>
     *
     * @param tableName
     *            void
     * @throws
     */
    public static void dropTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    /**
     * 对象转byte
     *
     * @param obj
     * @return
     */
    public static byte[] objectToByte(Object obj) {
        byte[] bytes = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream oo = new ObjectOutputStream(bo);
            oo.writeObject(obj);
            bytes = bo.toByteArray();
            bo.close();
            oo.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bytes;
    }

    /**
     * 把二进制数组的数据转回对象
     *
     * @param b
     * @return
     */
    public static Object restore(byte[] b) {
        ByteArrayInputStream bis = null;
        ObjectInputStream ois = null;
        try {
            bis = new ByteArrayInputStream(b);
            ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(ois);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        String family = "family1";
        String tableName = "demo_table";
        init();
        // 删除表
        dropTable(tableName);
        // 创建表
        createTable(tableName, family);
        addFamily(tableName, "family2");
        // 添加数据
        String row1 = "数据";
        Map<String, Object> columns = new HashMap<String, Object>();
        columns.put("test_data1", "test_data1_数据_1");
        columns.put("test_data11", "test_data1_数据_2");
        columns.put("test_data2", "test_data1_数据_3");
        addData(tableName, family, row1, columns);
        addData(tableName, "family2", row1, columns);

        // 添加数据
        String row2 = "ttt";
        Map<String, Object> columns2 = new HashMap<String, Object>();
        columns2.put("test_data1", "test_data1_ttt_1");
        columns2.put("test_data2", "test_data1_ttt_2");
        addData(tableName, family, row2, columns2);

        // 条件查询 family + row
        Map<String, String> rowMap1 = getValue(tableName, family, row1);
        System.out.println("查" + family + ":" + row1
                + "=============================");
        for (Map.Entry<String, String> entry : rowMap1.entrySet()) {
            System.out.println("rowMap1-" + entry.getKey() + ":"
                    + entry.getValue());
        }
        // 条件查询 family + row
        Map<String, String> rowMap2 = getValue(tableName, family, row2);
        System.out.println("查" + family + ":" + row2
                + "=============================");
        for (Map.Entry<String, String> entry : rowMap2.entrySet()) {
            System.out.println("rowMap2-" + entry.getKey() + ":"
                    + entry.getValue());
        }
        // 条件查询 family + row + column
        String data_value = getValue(tableName, family, row1, "test_data2");
        System.out.println("查" + family + ": " + row2 + ": test_data2的值"
                + "=============================");
        System.out.println("data_value->" + data_value);
        System.out.println("扫描全表=============================");
        // 查看表中所有数据
        getValue(tableName);
        // 删除行
        deleteRow(tableName, row2);
        // 查看表中所有数据
        System.out.println("删除行之后，扫描全表=============================");
        getValue(tableName);

        deleteFamily(tableName, family);
        System.out.println("删除列族 之后，扫描全表=============================");
        getValue(tableName);

    }

}
