package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TradeOrderBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.TimestampUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTradeOrderAddWindow {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String topic="dwd_trade_order_pre_process";
        String group="dwsTradeOrderAddWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {

                if (!"".equals(s)) {
                    JSONObject jsonObject = JSON.parseObject(s);
                    if("insert".equals(jsonObject.getString("type"))) {
                        collector.collect(jsonObject);
                    }
                }

            }
        });

        //过滤出最新的数据
        SingleOutputStreamOperator<JSONObject> jsonObjLatestDS = jsonObjDS.keyBy(json -> json.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<JSONObject> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<JSONObject>(
                                "last_order", JSONObject.class
                        ));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        TimerService timerService = context.timerService();
                        if (valueState.value() == null) {
                            valueState.update(jsonObject);
                            timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 2000L);
                        } else {
                            JSONObject value = valueState.value();
                            String rowTime1 = value.getString("row_op_ts");
                            String rowTime2 = jsonObject.getString("row_op_ts");
                            if (TimestampUtil.compare(rowTime1, rowTime2) < 0) {
                                valueState.update(jsonObject);
                            }
                        }


                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {

                        out.collect(valueState.value());
                        valueState.clear();
                    }
                });

        SingleOutputStreamOperator<JSONObject> jsonWithWMDS = jsonObjLatestDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(
                Duration.ofSeconds(2)
        ).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject jsonObject, long l) {
                String detailCreateTime = jsonObject.getString("detail_create_time");
                long ts = DateFormatUtil.dateTimeToTs(detailCreateTime);
                return ts;
            }
        }));

        SingleOutputStreamOperator<TradeOrderBean> uniqueAndNewUserDS = jsonWithWMDS.keyBy(jsonObject -> jsonObject.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {


                    ValueState<String> lastOrderState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastOrderState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeOrderBean>.Context context, Collector<TradeOrderBean> collector) throws Exception {

                        long newOrderUser = 0;
                        long todayOrderUser = 0;
                        String date= jsonObject.getString("detail_create_time").split(" ")[0];

                        if (lastOrderState.value() == null) {
                            newOrderUser = 1;
                            todayOrderUser = 1;
                            lastOrderState.update(date);
                        } else if (!date.equals(lastOrderState.value())) {
                            todayOrderUser = 1;
                            lastOrderState.update(date);
                        }
                        if (todayOrderUser != 0) {
                            Double split_original_total_amount = jsonObject.getDouble("split_original_total_amount");
                            Double split_activity_amount = jsonObject.getDouble("split_activity_amount");
                            Double split_coupon_amount = jsonObject.getDouble("split_coupon_amount");
                            split_activity_amount = split_activity_amount == null ? 0.00 : split_activity_amount;
                            split_coupon_amount = split_coupon_amount == null ? 0.00 : split_coupon_amount;
                            collector.collect(new TradeOrderBean("", "",
                                    todayOrderUser, newOrderUser, split_activity_amount,
                                    split_coupon_amount, split_original_total_amount,0L));
                        }


                    }
                });


        SingleOutputStreamOperator<TradeOrderBean> windowDS = uniqueAndNewUserDS
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean tradeOrderBean, TradeOrderBean t1) throws Exception {
                        tradeOrderBean.setOrderUniqueUserCount(tradeOrderBean.getOrderUniqueUserCount() + t1.getOrderUniqueUserCount());
                        tradeOrderBean.setOrderActivityReduceAmount(tradeOrderBean.getOrderActivityReduceAmount() + t1.getOrderActivityReduceAmount());
                        tradeOrderBean.setOrderNewUserCount(tradeOrderBean.getOrderNewUserCount() + t1.getOrderNewUserCount());
                        tradeOrderBean.setOrderOriginalTotalAmount(tradeOrderBean.getOrderOriginalTotalAmount() + t1.getOrderOriginalTotalAmount());
                        tradeOrderBean.setOrderCouponReduceAmount(tradeOrderBean.getOrderCouponReduceAmount() + t1.getOrderCouponReduceAmount());
                        return tradeOrderBean;
                    }
                }, new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>.Context context, Iterable<TradeOrderBean> iterable, Collector<TradeOrderBean> collector) throws Exception {
                        TradeOrderBean tradeOrderBean = iterable.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.toDateTime(window.getStart());
                        String edt = DateFormatUtil.toDateTime(window.getEnd());
                        tradeOrderBean.setStt(stt);
                        tradeOrderBean.setEdt(edt);
                        tradeOrderBean.setTs(System.currentTimeMillis());
                        collector.collect(tradeOrderBean);
                    }
                });


        windowDS.addSink(MyClickHouseUtil.sink("insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"));

        //windowDS.print();

        env.execute();


    }
}
