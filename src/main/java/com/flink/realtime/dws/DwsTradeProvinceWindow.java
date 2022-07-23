package com.flink.realtime.dws;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TradeProvinceOrderWindow;
import com.flink.realtime.func.DimAsyncJoinFunction;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.TimestampUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DwsTradeProvinceWindow {
    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 对之前kafka主题的数据进行处理
         */
        //读取kafka
        String topic="dwd_trade_order_pre_process";
        String group="dwsTradeProvinceWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));
        //过滤空值，只要insert
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                if (!"".equals(s)) {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if ("insert".equals(jsonObject.getString("type"))) {
                        collector.collect(jsonObject);
                    }
                }
            }
        });

        //状态编程，只要最新的leftjoin后的值
        SingleOutputStreamOperator<JSONObject> jsonLatestDS = jsonObjDS
                .keyBy(jsonObject -> jsonObject.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<JSONObject> latestState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        latestState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>("latest", JSONObject.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        String row_op_ts = jsonObject.getString("row_op_ts");
                        if (latestState.value() == null) {
                            latestState.update(jsonObject);
                            TimerService timerService = context.timerService();
                            long ts = TimestampUtil.fromTimeStampLtz(row_op_ts);
                            timerService.registerProcessingTimeTimer( ts+5000L);
                        } else {
                            JSONObject value = latestState.value();
                            String row_op_ts1 = value.getString("row_op_ts");
                            if (TimestampUtil.compare(row_op_ts, row_op_ts1) > 0) {
                                latestState.update(jsonObject);
                            }
                        }
                    }


                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        out.collect(latestState.value());
                        latestState.clear();
                    }
                });

        /**
         * 先开窗再关联，减少数据量,这里多了一次keyby,可以用hashset去重
         */
        //转化为JavaBean，提取WaterMark
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceDS = jsonLatestDS.keyBy(
                jsonObject -> jsonObject.getString("order_id")
        ).map(new RichMapFunction<JSONObject, TradeProvinceOrderWindow>() {
                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("order_id",
                                String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig
                                .newBuilder(org.apache.flink.api.common.time.Time.days(1))
                                .build());
                        valueState= getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public TradeProvinceOrderWindow map(JSONObject jsonObject) throws Exception {
                        long ts = DateFormatUtil.dateTimeToTs(jsonObject.getString("detail_create_time"));
                        TradeProvinceOrderWindow tradeProvinceOrderWindow =
                                new TradeProvinceOrderWindow("", "",
                                jsonObject.getString("province_id"),
                                "",
                                0L,
                                jsonObject.getDouble("split_total_amount"), ts);
                        if(valueState.value()==null){
                                valueState.update("1");
                                tradeProvinceOrderWindow.setOrderCount(1L);
                        }

                        return tradeProvinceOrderWindow;

                    }
                })


                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeProvinceOrderWindow>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2)).withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeProvinceOrderWindow>() {
                    @Override
                    public long extractTimestamp(TradeProvinceOrderWindow t1, long l) {

                        return t1.getTs();
                    }
                }));



        //开窗聚合
        SingleOutputStreamOperator<TradeProvinceOrderWindow> tradeProvinceWindowDS = tradeProvinceDS
                .keyBy(TradeProvinceOrderWindow::getProvinceId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeProvinceOrderWindow>() {
                    @Override
                    public TradeProvinceOrderWindow reduce(TradeProvinceOrderWindow t1, TradeProvinceOrderWindow t2) throws Exception {
                        t1.setOrderAmount(t1.getOrderAmount() + t2.getOrderAmount());
                        t1.setOrderCount(t1.getOrderCount() + t2.getOrderCount());
                        return t1;
                    }
                }, new ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeProvinceOrderWindow, TradeProvinceOrderWindow, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderWindow> iterable, Collector<TradeProvinceOrderWindow> collector) throws Exception {

                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.toDateTime(window.getStart());
                        String edt = DateFormatUtil.toDateTime(window.getEnd());
                        for (TradeProvinceOrderWindow tradeProvinceOrderWindow : iterable) {
                            tradeProvinceOrderWindow.setStt(stt);
                            tradeProvinceOrderWindow.setEdt(edt);
                            tradeProvinceOrderWindow.setTs(System.currentTimeMillis());
                            collector.collect(tradeProvinceOrderWindow);
                        }

                    }
                });
//        tradeProvinceWindowDS.print();

        //关联
        SingleOutputStreamOperator<TradeProvinceOrderWindow> resultDS = AsyncDataStream
                .unorderedWait(tradeProvinceWindowDS
                , new DimAsyncJoinFunction<TradeProvinceOrderWindow>("dim_base_province") {
                    @Override
                    public String getKey(TradeProvinceOrderWindow tradeProvinceOrderWindow) {
                        return tradeProvinceOrderWindow.getProvinceId();
                    }

                    @Override
                    public void join(TradeProvinceOrderWindow tradeProvinceOrderWindow, JSONObject jsonObject) {
                        tradeProvinceOrderWindow.setProvinceName(jsonObject.getString("NAME"));
                    }
                },
                60, TimeUnit.SECONDS);

        resultDS.print(">>>>>afterJoin");
        //sink
        resultDS.addSink(MyClickHouseUtil.sink("insert into dws_trade_province_order_window " +
                "values(?,?,?,?,?,?,?)"));
        env.execute();



    }
}
