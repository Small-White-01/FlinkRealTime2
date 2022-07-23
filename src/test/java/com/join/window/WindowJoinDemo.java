package com.join.window;

import com.join.interval.OrderDetail;
import com.join.interval.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinDemo {

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


        orderInfoDS.join(orderDetailDS)
                .where(OrderInfo::getId)
                .equalTo(OrderDetail::getOrderId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<OrderInfo, OrderDetail, String>() {
                    @Override
                    public String join(OrderInfo orderInfo, OrderDetail orderDetail) throws Exception {
                        return orderInfo+"=>"+orderDetail;
                    }
                }).print();
        env.execute();
    }
}
