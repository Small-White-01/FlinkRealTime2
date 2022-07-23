package com.flink.realtime.dwd;


import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 加购事实表+码表退化
 */
public class DwdCartInfoDetail {
    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //table建表
        String topic="topic_db";
        String groupId="dwdCartInfoDetail";
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(5));
        tableEnv.executeSql(
                 MyKafkaUtil.getTopicDB()+
                        MyKafkaUtil.getKafkaDDLConnectors(topic,groupId) +
                "");
//        //建cart_info
//        tableEnv.executeSql(
//                "create table cart_info( " +
//                "   `id` STRING, " +
//                "   `user_id` STRING, " +
//                "   `sku_id` STRING, " +
//                "   `cart_price` STRING, " +
//                "   `sku_num` INT, " +
//                "   `sku_name` STRING, " +
//                "   `create_time` STRING, " +
//                "   `is_order` STRING, " +
//                "   `order_time` STRING, " +
//                "   `source_type` STRING " +
//                " )");

//


//                tableEnv.sqlQuery("select * from cart_info").execute().print();;
        //过滤cart_info数据
        Table table = tableEnv.sqlQuery(
                "select  " +
                        "   data['id'] id, " +
                        "   data['user_id']  user_id," +
                        "   data['sku_id'] sku_id , " +
                        "   data['source_id']  source_id ," +
                        "   data['source_type']  source_type ," +
                        "   if(`type`='insert'," +
                        "data['sku_num'],cast(cast(`data`['sku_num'] as INT) - cast(`old`['sku_num'] as INT) as STRING)) sku_num, " +
                        "  ts ,   " +
                        "  proc_time    " +
                        "from `topic_db` " +
                        "where `table` = 'cart_info'  " +
                        "and (`type` = 'insert'  " +
                        "or (`type` = 'update'   " +
                        "and `old`['sku_num'] is not null   " +
                        "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))");

        tableEnv.createTemporaryView("cart_info",table);
//        tableEnv.executeSql("select * from cart_info").print();
        //建码表
//        String baseDic="base_dic";
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpTable());
//        tableEnv.createTemporaryView("my_base_dic",baseDic);

        //关联退化的码表
        Table table1 = tableEnv.sqlQuery("" +

                "select   " +
                "   c.id ,  " +
                "   c.user_id ,  " +
                "   c.sku_id ,  " +
                "   c.source_type ,  " +
                "  dic.dic_name," +
                "   cast(c.sku_num as INT) sku_num ,  " +
                "  ts    " +
                "from  " +
                "cart_info c   " +
                " left join  " +
                " my_base_dic FOR SYSTEM_TIME AS OF c.proc_time AS dic   " +
                "on c.source_type=dic.dic_code");
        ;
        tableEnv.createTemporaryView("result_tab",table1);
        //输出到kafka
        String sinkTopic="dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add(\n" +
                "   `id` STRING,\n" +
                "   `user_id` STRING,\n" +
                "   `sku_id` STRING,\n" +
                "   `source_type_code` STRING,\n" +
                "    source_type_name STRING,\n" +
                "       `sku_num` INT,\n" +
                "       `ts` STRING,\n" +
                "    primary key(id) not enforced \n" +
                " ) "+MyKafkaUtil.getUpdateKafkaDDLConnectors(sinkTopic));
//        tableEnv.executeSql();
      //  tableEnv.sqlQuery("select * from result_tab").execute().print();
        tableEnv.executeSql("insert into dwd_trade_cart_add " +
                "select * from result_tab");













    }

}
