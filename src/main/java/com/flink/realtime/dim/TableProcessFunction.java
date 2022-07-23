package com.flink.realtime.dim;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.HbaseConstants;
import com.flink.realtime.bean.TableProcess;
import com.flink.realtime.util.PhoenixUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.phoenix.jdbc.PhoenixDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    Connection connection;
    final List<String> typeList = Arrays.asList("bootstrap-insert", "insert", "update");
    public TableProcessFunction(MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;


    }

    @Override
    public void open(Configuration parameters) throws Exception {

        connection= PhoenixUtil.getConnection();
//            connection.setSchema(HbaseConstants.DATABASE);
    }

    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject,String,JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        ReadOnlyBroadcastState<String, TableProcess> readOnlyBroadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        String table = jsonObject.getString("table");


        //过滤字段
        TableProcess tableProcess = readOnlyBroadcastState.get(table);
        String type = jsonObject.getString("type");

        if(tableProcess!=null&&typeList.contains(type)){
            JSONObject data = jsonObject.getJSONObject("data");
            filterColumns(data,tableProcess.getSinkColumns());


            //如果是指定的Dim表插入数据到Hbase

            jsonObject.put("sinkTable",tableProcess.getSinkTable());

            //数据原样输出
            collector.collect(jsonObject);
        }
        else {
            System.out.println("过滤掉:"+jsonObject.toJSONString());
        }



    }

    private void filterColumns(JSONObject after, String sinkColumns) {

        String[] split = sinkColumns.split(",");
        List<String> list = Arrays.asList(split);
        after.entrySet().removeIf(stringObjectEntry -> !list.contains(stringObjectEntry.getKey()));

    }

    @Override
    public void processBroadcastElement(String json, BroadcastProcessFunction<JSONObject, String,JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        //
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONObject data = jsonObject.getJSONObject("after");
        if (data != null&&data.size()!=0) {
            TableProcess tableProcess = JSONObject.parseObject(data.toJSONString(), TableProcess.class);
            //建表
            createTable(tableProcess);
            //放入状态
            broadcastState.put(tableProcess.getSourceTable(), tableProcess);
        }
    }
    private void createTable(TableProcess tableProcess) {
        PreparedStatement preparedStatement=null;
        try {
            //拼接sql
            StringBuilder sb = new StringBuilder("" +
                    "create table if not exists ")
//                    .append(HbaseConstants.DATABASE)
//                    .append(".")
                    .append(tableProcess.getSinkTable())
                    .append("(");
            String sinkPk = tableProcess.getSinkPk();
            if (sinkPk == null || "".equals(sinkPk)) {
                sinkPk = "id";
            }
            String sinkColumns = tableProcess.getSinkColumns();
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                if (sinkPk.equals(column)) {
                    sb.append(column).append(" varchar primary key");
                } else {
                    sb.append(column).append(" varchar");
                }
                if (i < columns.length - 1) {
                    sb.append(",");
                }

            }
            String sinkExtend = tableProcess.getSinkExtend();
            sinkExtend = sinkExtend == null ? "" : sinkExtend;
            sb.append(")").append(sinkExtend);


            //建立表
            String sql = sb.toString();
            preparedStatement= connection.prepareStatement(sql);


            preparedStatement.execute();
            System.out.println("建表成功:"+sql);
        }catch (Exception e){
            throw new RuntimeException("建表失败,"+tableProcess.getSourceTable()+"" +
                    ":"+e.getMessage());
        }finally {
            if(preparedStatement!=null){
                try {
                    preparedStatement.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

    }
}
