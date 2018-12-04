package com.hks.hive.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveManage {

    private static final String URLHIVE = "jdbc:hive2://127.0.0.1:10000/default";
    private static Connection connection = null;


    public static Connection getHiveConnection() {
        if (null == connection) {
            synchronized (HiveManage.class) {
                if (null == connection) {
                    try {
                        Class.forName("org.apache.hive.jdbc.HiveDriver");
                        connection = DriverManager.getConnection(URLHIVE, "zhangsan", "123456789");
                    } catch (SQLException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        return connection;
    }


    public static void main(String args[]) throws SQLException{
        try{
            String sql1="select ipaddress ,t_user,request,agent from apachelog limit 5";
            PreparedStatement pstm = getHiveConnection().prepareStatement(sql1);
            ResultSet rs= pstm.executeQuery(sql1);

            while (rs.next()) {
                System.out.println(rs.getString(1)+"	"+rs.getString(2)+
                        "		"+rs.getString(3)+"		"+rs.getString(4));
            }
            pstm.close();
            rs.close();
        }
        catch (Exception ex){
            ex.printStackTrace();
        }
    }


}

