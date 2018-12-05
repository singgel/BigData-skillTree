package com.hks.hive.test;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveCreateTable {
    private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws SQLException, Exception {

        // Register driver and create driver instance
        Class.forName(driverName);

        // get connection
        Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/userdb", "", "");

        // create statement
        Statement stmt = con.createStatement();

        // execute statement
        stmt.executeQuery("CREATE TABLE IF NOT EXISTS "
                +" employee ( eid int, name String, "
                +" salary String, destignation String)"
                +" COMMENT 'Employee details'"
                +" ROW FORMAT DELIMITED"
                +" FIELDS TERMINATED BY '\t'"
                +" LINES TERMINATED BY '\n'"
                +" STORED AS TEXTFILE;");

        System.out.println("Table employee created.");
        con.close();
    }
}
