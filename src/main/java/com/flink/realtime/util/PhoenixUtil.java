package com.flink.realtime.util;

import com.flink.realtime.bean.HbaseConstants;

import java.sql.*;

public class PhoenixUtil {
    static {
        try {
            Class.forName(HbaseConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    public static Connection getConnection(){
        try {
            return DriverManager.getConnection(HbaseConstants.URL);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public static void main(String[] args) throws SQLException {
        Connection connection = getConnection();
        ResultSet catalogs = connection.getMetaData().getCatalogs();
        ResultSetMetaData metaData = catalogs.getMetaData();
        while (catalogs.next()){
            for (int i = 0; i < metaData.getColumnCount(); i++) {

                String columnName = metaData.getColumnName(i + 1);
                String val = catalogs.getString(columnName);
                System.out.println(columnName+"=>"+val);
            }
        }
        catalogs.close();
        connection.close();
    }


}
