package com.flink.realtime.dwd;

import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * 支付成功明细表
 */
public class DwdTradePaymentDetail {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        String group="dwdTradePaymentDetail";
        //要关联的维表
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
                " row_op_ts TIMESTAMP_LTZ(3)\n"+
                ")"+ MyKafkaUtil.getKafkaDDLConnectors("dwd_trade_order_detail",
        group));


        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpTable());



        //取payment数据
        tableEnv.executeSql(MyKafkaUtil.getTopicDB()+MyKafkaUtil.getKafkaDDLConnectors("topic_db",
                group));
        Table paymentInfo = tableEnv.sqlQuery("" +
                "select\n" +
                "  data['user_id'] user_id,\n" +
                "  data['order_id'] order_id,\n" +
                "  data['payment_type'] payment_type,\n" +
                "data['callback_time'] callback_time,\n" +
                "`proc_time`,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'payment_info' and data['callback_time'] is not null\n");
        tableEnv.createTemporaryView("payment_info",paymentInfo);
        //tableEnv.sqlQuery("select * from payment_info limit 10").execute().print();
//
        Table paymentResult = tableEnv.sqlQuery(" select\n" +
                "  od.id detail_id,  " +
                "  od.order_id,  " +
                "  od.user_id,  " +
                "  od.sku_id,  " +
                "  od.province_id,  " +
                "  od.source_id,  " +
                "  od.source_type,  " +
                "  od.source_type_name,  " +
                "  od.activity_id,      " +
                "  od.activity_rule_id,      " +
                "  od.coupon_id,      " +
                "  pi.payment_type payment_type_code,      " +
                "  dic.dic_name payment_type_name,      " +
                "  pi.callback_time,      " +
                "  od.sku_num,  " +
                "  od.split_original_total_amount,  " +
                "  od.split_total_amount split_payment_amount,  " +
                "  od.split_activity_amount,  " +
                "  od.split_coupon_amount,  " +
                "  pi.ts      " +
                " from payment_info pi \n" +
                " join dwd_trade_order_detail od\n" +
                " on pi.order_id=od.order_id\n" +
                " left join my_base_dic FOR SYSTEM_TIME AS OF pi.proc_time  as dic\n" +
                " on pi.payment_type=dic.dic_code");

        tableEnv.createTemporaryView("paymentResult",paymentResult);
        //tableEnv.sqlQuery("select * from paymentResult").execute().print();
        tableEnv.executeSql("" +
                "create table dwd_trade_pay_detail_suc(\n" +
                "order_detail_id string,\n" +
                "order_id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "activity_id string,\n" +
                "activity_rule_id string,\n" +
                "coupon_id string,\n" +
                "payment_type_code string,\n" +
                "payment_type_name string,\n" +
                "callback_time string,\n" +
                "sku_num string,\n" +
                "split_original_amount string,\n" +
                "split_payment_amount string,\n" +
                "split_activity_amount string,\n" +
                "split_coupon_amount string,\n" +
                "ts string,\n" +
                "primary key(order_detail_id) not enforced\n" +
                ")" +MyKafkaUtil.getUpdateKafkaDDLConnectors("dwd_trade_pay_detail_suc"));

       tableEnv.executeSql("insert into dwd_trade_pay_detail_suc select * from paymentResult").print();
        env.execute();
    }
}
