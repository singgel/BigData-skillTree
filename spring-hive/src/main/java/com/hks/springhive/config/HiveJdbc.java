package com.hks.springhive.config;

import java.sql.*;

/**
 * @Author: hekuangsheng
 * @Date: 2018/11/1
 */
public class HiveJdbc {
    // jdbc驱动路径
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    // hive库地址+库名
    private static String url = "jdbc:hive2://192.168.70.3:10000/default";
    // 用户名
    private static String user = "";
    // 密码
    private static String password = "";

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet res = null;
        try {
            conn = getConn();
            System.out.println(conn);
            stmt = conn.createStatement();
            stmt.execute("drop table hivetest");
            stmt.execute("CREATE TABLE if not EXISTS hivetest(" +
                    "ymd date," +
                    "price_open FLOAT ," +
                    "price_high FLOAT ," +
                    "price_low FLOAT ," +
                    "price_close float," +
                    "volume int," +
                    "price_adj_close FLOAT" +
                    ")partitioned by (exchanger string,symbol string)" +
                    "row format delimited fields terminated by ','");
            stmt.execute("LOAD DATA LOCAL INPATH '/home/mfz/apache-hive-2.1.1-bin/hivedata/stocks.csv' " +
                    "OVERWRITE INTO TABLE hivetest partition(exchanger=\"NASDAQ\",symbol=\"INTC\")");
            res = stmt.executeQuery("select * from hivetest limit 10");
            System.out.println("执行 select * query 运行结果:");
            while (res.next()) {
                System.out.println(
                        "日期:" + res.getString(1) +
                                "|price_open:" + res.getString(2) +
                                "|price_hign：" + res.getString(3) +
                                "|price_low：" + res.getString(4) +
                                "|price_close：" + res.getString(5) +
                                "|volume:" + res.getString(6) +
                                "|price_adj_close:" + res.getString(7) +
                                "|exchanger:" + res.getString(8) +
                                "|symbol:" + res.getString(9));
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            try {
                if (null != res) {
                    res.close();
                }
                if (null != stmt) {
                    stmt.close();
                }
                if (null != conn) {
                    conn.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }
}
