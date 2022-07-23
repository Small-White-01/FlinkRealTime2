package com.connect;

import com.join.interval.OrderDetail;
import com.join.interval.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

public class ConnectDemo {

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

//        orderInfoDS.print();
//        orderDetailDS.print();
       orderInfoDS.connect(orderDetailDS)
                       .keyBy(OrderInfo::getId, OrderDetail::getOrderId)
                               .process(new MyCoProcessFunction(Time.days(1),Time.days(1))).print();



        env.execute();


    }

    static class MyCoProcessFunction extends KeyedCoProcessFunction<String, OrderInfo, OrderDetail, String> {

        ValueState<OrderInfo> orderInfoValueState;
        ListState<OrderDetail> orderDetailValueState;
        private Time time1;
        private Time time2;
        public MyCoProcessFunction(Time time1,Time time2) {
            this.time1 = time1;
            this.time2=time2;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<OrderInfo> orderInfoState = new ValueStateDescriptor<>(
                    "orderInfo", OrderInfo.class
            );
            orderInfoState.enableTimeToLive(
                    StateTtlConfig.newBuilder(time1)
                            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                            .build()
            );
            orderInfoValueState=getRuntimeContext().getState(orderInfoState);
            ListStateDescriptor<OrderDetail> orderDetailState = new ListStateDescriptor<>(
                    "orderDetail", OrderDetail.class
            );
            orderDetailState.enableTimeToLive(StateTtlConfig.newBuilder(time2)
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .build());
            orderDetailValueState=getRuntimeContext().getListState(orderDetailState);
        }

        @Override
        public void processElement1(OrderInfo orderInfo, Context
        context, Collector<String> collector) throws Exception {

            if(orderDetailValueState.get()!=null){
                Iterable<OrderDetail> orderDetails = orderDetailValueState.get();
                for (OrderDetail orderDetail:orderDetails) {
                    collector.collect(orderInfo + "=>" + orderDetail);
                }
                orderDetailValueState.clear();
            }
            orderInfoValueState.update(orderInfo);
            context.timerService().registerEventTimeTimer(orderInfo.getTs()+time1.toMilliseconds());




        }

        @Override
        public void processElement2(OrderDetail orderDetail, Context
        context, Collector<String> collector) throws Exception {

            if(orderInfoValueState.value()!=null){
                OrderInfo orderInfo = orderInfoValueState.value();
                collector.collect(orderInfo + "=>" + orderDetail);
//                context.timerService().deleteEventTimeTimer(orderInfo.getTs()+time.toMilliseconds());
//                context.timerService().registerEventTimeTimer();
            }else {
                orderDetailValueState.add(orderDetail);
                long l = time2.toMilliseconds();
                long expire = orderDetail.getTs() / l * l;
                context.timerService().registerEventTimeTimer(expire);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            String result="";
            OrderInfo value = orderInfoValueState.value();
            result=value==null?"":value.toString();
            Iterable<OrderDetail> orderDetails = orderDetailValueState.get();
            if(orderDetails!=null){
                for (OrderDetail orderDetail:orderDetails)
                    out.collect(result+"=>"+orderDetail);
            }
            orderInfoValueState.clear();
            orderDetailValueState.clear();

        }
    }
}
