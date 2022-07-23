package com.join;

import com.join.interval.OrderDetail;
import com.join.interval.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 自定义连接，leftjoin,rightjoin ,join的底层即coGroup
 */
public class CoGroupDemo {

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

        orderInfoDS.coGroup(orderDetailDS)
                .where(r->r.getId())
                .equalTo(r->r.getOrderId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<OrderInfo, OrderDetail, String>() {
                    @Override
                    public void coGroup(Iterable<OrderInfo> iterable, Iterable<OrderDetail> iterable1, Collector<String> collector) throws Exception {
                        collector.collect(iterable+"=>"+iterable1);
                    }

                }).print();
//        orderInfoDS.join(orderDetailDS)
//                .where(r->r.getId())
//                .equalTo(r->r.getOrderId())
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .apply(new FlatJoinFunction<OrderInfo, OrderDetail, String>() {
//                    @Override
//                    public void join(OrderInfo orderInfo, OrderDetail orderDetail, Collector<String> collector) throws Exception {
//
//                    }
//                })
        env.execute();
    }
}
