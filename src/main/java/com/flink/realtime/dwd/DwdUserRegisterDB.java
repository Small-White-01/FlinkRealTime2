package com.flink.realtime.dwd;

import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DwdUserRegisterDB {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String topic="topic_db";
        String group="dwd_user_register";
        tableEnv.executeSql(MyKafkaUtil.getTopicDB()+
                MyKafkaUtil.getKafkaDDLConnectors(topic,group));

        Table userInfo = tableEnv.sqlQuery("" +
                "select " +
                "data['id'] user_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                " from topic_db" +
                " where `table`='user_info' and `type`='insert'" +
                "");
        tableEnv.createTemporaryView("user_info",userInfo);

        // TODO 5. 创建 Upsert-Kafka dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`(\n" +
                "`user_id` string,\n" +
                "`date_id` string,\n" +
                "`create_time` string,\n" +
                "`ts` string,\n" +
                "primary key(`user_id`) not enforced\n" +
                ")" + MyKafkaUtil.getUpdateKafkaDDLConnectors("dwd_user_register"));

        // TODO 6. 将输入写入 Upsert-Kafka 表
        tableEnv.executeSql("insert into dwd_user_register\n" +
                "select \n" +
                "user_id,\n" +
                "date_format(create_time, 'yyyy-MM-dd') date_id,\n" +
                "create_time,\n" +
                "ts\n" +
                "from user_info");

    }

}
