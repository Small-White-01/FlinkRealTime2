package com.flink.realtime.dwd;

import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

public class DwdTradeOrderDetail {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        String topic="dwd_trade_order_pre_process";
        String group="dwdTradeOrderDetail";
        tableEnv.executeSql("create table dwd_trade_order_pre_process(\n" +
                "  id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  sku_id STRING,\n" +
                        "  source_id STRING,\n" +
                        "  source_type STRING,\n" +
                        "  detail_create_time STRING,\n" +
                        "  order_price STRING,\n" +
                        "  sku_num STRING,\n" +
                        "  split_original_total_amount STRING,\n" +
                        "  split_total_amount STRING,\n" +
                        "  split_activity_amount STRING,\n" +
                        "  split_coupon_amount STRING,\n" +
                        "  proc_time TIMESTAMP_LTZ(3),\n" +
                        "  user_id STRING,\n" +
                        "  province_id STRING,\n" +
                        "  `type`  STRING,\n" +
                        "  `old` MAP<STRING,STRING>,\n" +
                        "  activity_id STRING,\n" +
                        "  activity_rule_id STRING,\n" +
                        "  activity_create_time STRING,\n" +
                        "  coupon_id STRING,\n" +
                        "  coupon_use_id STRING,\n" +
                        "  coupon_create_time STRING,\n" +
                        "  source_type_name STRING,\n" +
                        " row_op_ts TIMESTAMP_LTZ(3)\n"+

                        ")"+ MyKafkaUtil.getKafkaDDLConnectors(topic,group));


        Table orderDetail = tableEnv.sqlQuery("select   " +
                "  id ,  " +
                "  order_id,  " +
                "  sku_id,  " +
                "  source_id,  " +
                "  source_type,  " +
                "  detail_create_time ,  " +
                "  order_price,  " +
                "  sku_num,  " +
                "  split_original_total_amount,  " +
                "  split_total_amount,  " +
                "  split_activity_amount,  " +
                "  split_coupon_amount,  " +
                "  proc_time,  " +
                "  user_id,  " +
                "  province_id,  " +
                "  activity_id,      " +
                "  activity_rule_id,      " +
                "  activity_create_time,      " +
                "  coupon_id,      " +
                "  coupon_use_id,      " +
                "  coupon_create_time,     " +
                "  source_type_name,      " +
                "row_op_ts " +
                "from dwd_trade_order_pre_process " +
                "where `type` ='insert'"
        );
        tableEnv.createTemporaryView("order_detail",orderDetail);

        tableEnv.executeSql("create table dwd_trade_order_detail(\n" +
                "  id STRING,\n" +
                "  order_id STRING,\n" +
                "  sku_id STRING,\n" +
                "  source_id STRING,\n" +
                "  source_type STRING,\n" +
                "  detail_create_time STRING,\n" +
                "  order_price STRING,\n" +
                "  sku_num STRING,\n" +
                "  split_original_total_amount STRING,\n" +
                "  split_total_amount STRING,\n" +
                "  split_activity_amount STRING,\n" +
                "  split_coupon_amount STRING,\n" +
                "  proc_time TIMESTAMP_LTZ(3),\n" +
                "  user_id STRING,\n" +
                "  province_id STRING,\n" +
                "  activity_id STRING,\n" +
                "  activity_rule_id STRING,\n" +
                "  activity_create_time STRING,\n" +
                "  coupon_id STRING,\n" +
                "  coupon_use_id STRING,\n" +
                "  coupon_create_time STRING,\n" +
                "  source_type_name STRING,\n" +
                " row_op_ts TIMESTAMP_LTZ(3),\n"+
                "  primary key(id) not enforced\n" +
                ")"+ MyKafkaUtil.getUpdateKafkaDDLConnectors("dwd_trade_order_detail"));

        tableEnv.executeSql("insert into dwd_trade_order_detail select * from order_detail")
                .print();
        env.execute();

    }
}
