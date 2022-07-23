package com.join.interval;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderInfo> orderInfoDS = env.socketTextStream("hadoop2", 9999)
                .map(text -> {
                    String[] s = text.split(" ");
                    return new OrderInfo(s[0], s[1], Long.parseLong(s[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                                .withTimestampAssigner((orderInfo, l) -> orderInfo.getTs() * 1000)
                );

        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.socketTextStream("hadoop2", 8888)
                .map(text -> {
                    String[] s = text.split(" ");
                    return new OrderDetail(s[0], s[1], Long.parseLong(s[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner((orderDetail, l) -> orderDetail.getTs() * 1000)
                );

        KeyedStream<OrderInfo, String> orderInfoKeyDS = orderInfoDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, String> orderDetailKeyDS = orderDetailDS.keyBy(OrderDetail::getOrderId);
        SingleOutputStreamOperator<String> orderDS = orderInfoKeyDS.intervalJoin(orderDetailKeyDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, String>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, ProcessJoinFunction<OrderInfo, OrderDetail, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(orderInfo.toString() + "=>" + orderDetail.toString());
                    }
                });

        orderDS.print(">>>>");


        env.execute();


    }
}
