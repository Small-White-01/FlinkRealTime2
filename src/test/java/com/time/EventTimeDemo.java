package com.time;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.time.Duration;

public class EventTimeDemo {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop2", 9999);

        SingleOutputStreamOperator<Bean> beanWaterMarkDS = socketDS.map(event -> {
            String[] s = event.split(" ");
            return new Bean(s[0], s[1],s[2], Long.parseLong(s[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Bean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Bean>() {
                    @Override
                    public long extractTimestamp(Bean bean, long l) {
                        return bean.ts;
                    }
                }));


        SingleOutputStreamOperator<Bean> filterLatestDS = beanWaterMarkDS.keyBy(bean -> bean.someId)
                .process(new KeyedProcessFunction<String, Bean, Bean>() {

                    ValueState<Bean> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Bean>("bean", Bean.class));
                    }

                    @Override
                    public void processElement(Bean bean, KeyedProcessFunction<String, Bean, Bean>.Context context, Collector<Bean> collector) throws Exception {
                        if (valueState.value() == null) {
                            valueState.update(bean);
                            context.timerService().registerProcessingTimeTimer( 5000);
                        } else {
                            Long ts = bean.ts;
                            Long ts1 = valueState.value().ts;
                            if (ts > ts1) valueState.update(bean);
                        }

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Bean, Bean>.OnTimerContext ctx, Collector<Bean> out) throws Exception {

                        out.collect(valueState.value());
                        valueState.clear();
                    }
                });

        SingleOutputStreamOperator<Tuple2<String, Long>> resultDS = filterLatestDS.keyBy(bean -> bean.name)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Bean, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(Bean bean, Long aLong) {
                                   return aLong + 1;
                               }

                               @Override
                               public Long getResult(Long aLong) {
                                   return aLong;
                               }

                               @Override
                               public Long merge(Long aLong, Long acc1) {
                                   return null;
                               }
                           },

                        new ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                                Long ct = iterable.iterator().next();
                                collector.collect(Tuple2.of(s, ct));
                            }
                        });
        resultDS.print();


        env.execute();

    }
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Bean{
        private String someId;
        private String name;
        private String url;
        private Long ts;
    }
}
