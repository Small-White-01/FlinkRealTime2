package com.join.table;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class JoinDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

      //  tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<Tuple3<String,String,Long>> DS1 = env.socketTextStream("hadoop2", 8888)
                .map(text -> {
                    String[] s = text.split(" ");
                    return Tuple3.of(s[0], s[1], Long.parseLong(s[2]));
                }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.LONG));

        SingleOutputStreamOperator<Tuple3<String,String,Long>> DS2 = env.socketTextStream("hadoop2", 9999).map(text -> {
            String[] s = text.split(" ");
            return Tuple3.of(s[0], s[1], Long.parseLong(s[2]));
        }).returns(Types.TUPLE(Types.STRING,Types.STRING,Types.LONG));
//        DS1.print(">>>>1");
//        DS2.print(">>>>2");
//
//        Table table = tableEnv.fromDataStream(DS1);
//        Table table1 = tableEnv.fromDataStream(DS2);
        tableEnv.createTemporaryView("A",DS1,$("id"),$("name"),$("ts"));
        tableEnv.createTemporaryView("B",DS2,$("orderId"),$("orderPrice"),$("ts1"));
//
//
        tableEnv.sqlQuery("select A.id,A.name,B.orderPrice from A left join B on A.id = B.orderId")
                .execute().print();
//        tableEnv.sqlQuery("select * from B")
//                .execute().print();


        env.execute();

    }
    @Data
    @AllArgsConstructor
    public static class BeanA{

        public String id;
        public String name;
        public Long ts;
    }
    @Data
    @AllArgsConstructor
   public static class BeanB{
         public String orderId;
         public String orderPrice;
         public Long ts1;
    }

}
