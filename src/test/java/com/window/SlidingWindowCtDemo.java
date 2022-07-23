package com.window;

import com.flink.realtime.util.BloomFilterUtil;
import com.flink.realtime.util.DateFormatUtil;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.join.interval.OrderDetail;
import com.join.interval.OrderInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.Charset;

public class SlidingWindowCtDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<OrderDetail> orderDetailDS = env.socketTextStream("hadoop2", 8888)
                .map(text -> {
                    String[] s = text.split(" ");
                    return new OrderDetail(s[0], s[1], Long.parseLong(s[2]));
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                                .withTimestampAssigner((orderDetail, l) -> orderDetail.getTs())
                );


        //无论是否keyby，不同skuid的水位线会更新其他skuid的水位线，即广播
        orderDetailDS.keyBy(OrderDetail::getSkuId)
                .window(SlidingEventTimeWindows.of(Time.seconds(6), Time.seconds(2)))
                .aggregate(new AggregateFunction<OrderDetail, OrderCt, OrderCt>() {
                    @Override
                    public OrderCt createAccumulator() {
                        return new OrderCt();
                    }

                    @Override
                    public OrderCt add(OrderDetail orderDetail, OrderCt orderCt) {
                        String ts = orderDetail.getTs() + "";
                        boolean contain = BloomFilterUtil.contain(ts);
                        if(!contain){
                            BloomFilterUtil.put(orderDetail.getTs()+"");
//                            orderCt.setCt(orderCt.getCt() + 1);
//                            orderCt.add(orderDetail.toString());
                        }
//                        if(!BufferSet.exist(orderDetail.toString())) {
//                            BufferSet.sAdd(orderDetail.toString());
//                        }
                        return orderCt;
                    }

                    @Override
                    public OrderCt getResult(OrderCt orderCt) {
//                        if(BloomFilterUtil.getElementCount()>500){
//                            BloomFilterUtil.renew();
//                        }

                        orderCt.setCt(BloomFilterUtil.getElementCount());
                        BloomFilterUtil.renew();
//                        BufferSet.clear();
                        return orderCt;
                    }

                    @Override
                    public OrderCt merge(OrderCt orderCt, OrderCt acc1) {
                        return null;
                    }
                }, new ProcessWindowFunction<OrderCt, OrderCt, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<OrderCt, OrderCt, String, TimeWindow>.Context context, Iterable<OrderCt> iterable, Collector<OrderCt> collector) throws Exception {
                        OrderCt orderCt = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.toDateTime(window.getStart());
                        String edt = DateFormatUtil.toDateTime(window.getEnd());
                        orderCt.setSkuId(s);
                        orderCt.setStt(stt);
                        orderCt.setEdt(edt);

                        collector.collect(orderCt);
                    }
                }).print();
        env.execute();
    }

    static class BloomFilterAggregateFunction extends RichAggregateFunction<OrderDetail, OrderCt, OrderCt> {

        private static BloomFilter<String> bloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10000, 0.01);
        }

        @Override
        public OrderCt createAccumulator() {
            return new OrderCt();
        }

        @Override
        public OrderCt add(OrderDetail orderDetail, OrderCt orderCt) {
            String ts = orderDetail.getTs() + "";
            boolean b = bloomFilter.mightContain(ts);

            if (!b) {
                orderCt.setCt(orderCt.getCt() + 1);
                orderCt.add(orderDetail.toString());
                bloomFilter.put(ts);
            }

            return orderCt;
        }

        @Override
        public OrderCt getResult(OrderCt orderCt) {
//                        int setSize = bloomFilterOrderCtTuple2.f1.getSetSize();
//                        bloomFilterOrderCtTuple2.f1.setCt(setSize);
            if(bloomFilter.approximateElementCount()>500){
                bloomFilter=BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10000, 0.01);
            }
            return orderCt;
        }

        @Override
        public OrderCt merge(OrderCt orderCt, OrderCt acc1) {
            return null;
        }
    }
}
