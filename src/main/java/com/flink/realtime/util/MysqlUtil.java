package com.flink.realtime.util;

public class MysqlUtil {

    public static final String DEFAULT_DATABASE="gmallflink";
    public static String getMysqlLookUpConnectorString(String table){
        return getMysqlConnectorString(DEFAULT_DATABASE,table);
    }
    public static String getMysqlConnectorString(String database,String table){
        return "WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop2:3306/"+database+"',\n" +
                "   'username' = 'root',\n" +
                "   'password' = 'root',  \n" +
                "'lookup.cache.max-rows' = '10',\n" +
                "'lookup.cache.ttl' = '1 hour',\n" +

                "   'driver'='com.mysql.cj.jdbc.Driver',\n"+
                "   'table-name' = '"+table+"'\n" +
                ")";
    }

    public static String getBaseDicLookUpTable(){
        return "" +
                "CREATE TABLE my_base_dic (  " +
                "  dic_code STRING,  " +
                "  dic_name STRING,  " +
                "  parent_code STRING,  " +
                "  PRIMARY KEY (dic_code) NOT ENFORCED  " +
                ") "+ MysqlUtil.getMysqlLookUpConnectorString("base_dic");
    }
}
