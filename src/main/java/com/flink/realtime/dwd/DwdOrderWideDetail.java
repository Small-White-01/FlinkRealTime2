package com.flink.realtime.dwd;

import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.ZoneId;

/**
 * 订单宽表：订单明细表，数据量最全，作为主表，left join 其他表，
 *         订单表，订单活动表，订单优惠券表
 */
public class DwdOrderWideDetail {

    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Table读取kafka
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        String topic="topic_db";
        String group="dwdOrderWideDetail";
        tableEnv.executeSql(MyKafkaUtil.getTopicDB()+
                MyKafkaUtil.getKafkaDDLConnectors(topic,group));

        //分别查询建表
        Table t_orderDetail = tableEnv.sqlQuery("" +
                "select   " +
                "   data['id'] id,  " +
                "   data['order_id'] order_id,  " +
                "   data['sku_id'] sku_id,  " +
                "   data['sku_name'] sku_name,  " +
                "   data['source_id'] source_id,  " +
                "   data['source_type'] source_type,  " +
                "   data['create_time'] create_time,  " +
                "   data['order_price'] order_price,  " +
                "   data['sku_num']  sku_num,  " +
                "   cast(cast(data['order_price'] as DECIMAL(16,2)) * cast(data['sku_num'] as INT) as STRING) split_original_total_amount,  " +
                "   data['split_total_amount'] split_total_amount,  " +
                "   data['split_activity_amount'] split_activity_amount,  " +
                "   data['split_coupon_amount'] split_coupon_amount,  " +
                "   proc_time " +
                "from   " +
                "topic_db  " +
                "where `table`='order_detail' and type='insert'");
        tableEnv.createTemporaryView("order_detail",t_orderDetail);

        Table t_orderInfo = tableEnv.sqlQuery("select  " +
                "   data['id'] id,  " +
                "   data['user_id'] user_id,  " +
                "   data['consignee'] consignee,  " +
                "   data['province_id'] province_id,  " +
                "data['operate_time'] operate_time,  " +
                "data['order_status'] order_status,  " +

                "   `type` ,  " +
                "   `old`   " +
                "from   " +
                "topic_db  " +
                "where `table`='order_info' and (type='insert'  " +
                "       or type='update')");
//        Table t_orderActivity = tableEnv.sqlQuery();
//        Table t_orderCoupon = tableEnv.sqlQuery();
        tableEnv.createTemporaryView("order_info",t_orderInfo);
        // TODO 6. 读取订单明细活动关联表数据
        Table orderDetailActivity = tableEnv.sqlQuery("select   " +
                "data['id'] id,  " +
                "data['order_id'] order_id,  " +
                "data['order_detail_id'] order_detail_id,  " +
                "data['activity_id'] activity_id,  " +
                "data['activity_rule_id'] activity_rule_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['create_time'] create_time  " +
                "from `topic_db`  " +
                "where `table` = 'order_detail_activity'  " +
                "and `type` = 'insert'  ");
        tableEnv.createTemporaryView("order_detail_activity", orderDetailActivity);

        // TODO 7. 读取订单明细优惠券关联表数据
        Table orderDetailCoupon = tableEnv.sqlQuery("select  " +
                "data['id'] id,  " +
                "data['order_id'] order_id,  " +
                "data['order_detail_id'] order_detail_id,  " +
                "data['coupon_id'] coupon_id,  " +
                "data['coupon_use_id'] coupon_use_id,  " +
                "data['sku_id'] sku_id,  " +
                "data['create_time'] create_time  " +
                "from `topic_db`  " +
                "where `table` = 'order_detail_coupon'  " +
                "and `type` = 'insert'  ");
        tableEnv.createTemporaryView("order_detail_coupon", orderDetailCoupon);


        //码表读取
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpTable());


        //leftjoin
        Table resultTab = tableEnv.sqlQuery("" +
                "select   " +
                "  od.id ,  " +
                "  od.order_id,  " +
                "  od.sku_id,  " +
                "  od.source_id,  " +
                "  od.source_type,  " +
                "  od.create_time detail_create_time,  " +
                "  od.order_price,  " +
                "  od.sku_num,  " +
                "  od.split_original_total_amount,  " +
                "  od.split_total_amount,  " +
                "  od.split_activity_amount,  " +
                "  od.split_coupon_amount,  " +
                "  od.proc_time,  " +
                "  oi.user_id,  " +
                "  oi.province_id,  " +
//                "  oi.consignee,  " +
                "  oi.`type` ,  " +
                "  oi.`old`,  " +
                "  oda.activity_id,      " +
                "  oda.activity_rule_id,      " +
                "  oda.create_time activity_create_time,      " +
                "  odc.coupon_id,      " +
                "  odc.coupon_use_id,      " +
                "  odc.create_time coupon_create_time ,     " +
                "  dic.dic_code source_type_name,      " +
                //对每一行加入时间戳，由于leftjoin发往kafka时会产生多条  左->空->空，左->右->空，左->右->右,
                //只取最新值，由于proctime已经早生成了，对这三条数据都是一样的，无法区别这三条哪个最新
                "current_row_timestamp() row_op_ts\n"+
                "from   " +
                "order_detail od  " +
                "left join order_info oi on od.order_id=oi.id   " +
                "left join order_detail_activity oda on od.id=oda.order_detail_id  " +
                "left join order_detail_coupon odc on od.id=odc.order_detail_id  " +
                "left join my_base_dic FOR SYSTEM_TIME AS OF od.proc_time AS dic   " +
                "on od.source_type=dic.dic_code  ");
        tableEnv.createTemporaryView("resultTab",resultTab);

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
                " row_op_ts TIMESTAMP_LTZ(3),\n"+
                "  primary key(id) not enforced\n" +
                ")"+MyKafkaUtil.getUpdateKafkaDDLConnectors("dwd_trade_order_pre_process"));

//        //sink
//        Table table = tableEnv.sqlQuery("select * from resultTab limit 10");
//        tableEnv.toChangelogStream(table).print();
        tableEnv.executeSql("insert into dwd_trade_order_pre_process select * from resultTab")
                .print();
        env.execute();
    }
}
