package com.flink.realtime.dim;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.HbaseConstants;
import com.flink.realtime.util.DIMUtil;
import com.flink.realtime.util.PhoenixUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PMetaData;

import java.sql.*;
import java.util.Collection;
import java.util.Set;

public class HbaseSink extends RichSinkFunction<JSONObject> {

    PhoenixConnection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        connection= (PhoenixConnection) PhoenixUtil.getConnection();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        String sql=buildSql(value);
        PreparedStatement preparedStatement = null;
        try {

            preparedStatement = connection.prepareStatement(sql);
           // preparedStatement.execute();
            preparedStatement.executeUpdate();
            connection.commit();
            String type = value.getString("type");
            if("update".equals(type)){
                String table = value.getString("table");
                String id = value.getJSONObject("data").getString("id");
                DIMUtil.delKey(table,id);
            }

//            ResultSet catalogs = connection.getMetaData().getCatalogs();
//            ResultSetMetaData metaData = catalogs.getMetaData();
//            while (catalogs.next()){
//                for (int i = 0; i < metaData.getColumnCount(); i++) {
//
//                    String columnName = metaData.getColumnName(i + 1);
//                    String val = catalogs.getString(columnName);
//                    System.out.println(columnName+"=>"+val);
//                }
//            }

        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println(sql+":执行失败:"+e.getMessage());
            connection.rollback();
        }finally {
            assert preparedStatement != null;
            preparedStatement.close();
        }


    }

    private String buildSql(JSONObject value) {
        JSONObject data = value.getJSONObject("data");
        Set<String> set = data.keySet();
        Collection<Object> values = data.values();
        String sinkTable = value.getString("sinkTable");
        return "upsert into "+ HbaseConstants.DATABASE+"."+sinkTable+" ("+
                StringUtils.join(set,',')+") values ('"+
                StringUtils.join(values,"','")+"')";
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
