package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TradePaymentWindowBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
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

public class DwsTradePaymentUserCtWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String topic="dwd_trade_pay_detail_suc";
        String group="dwsTradePaymentUserCtWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));

        SingleOutputStreamOperator<JSONObject> jsonWithWmDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts") * 1000;
                            }
                        }));


        SingleOutputStreamOperator<TradePaymentWindowBean> newAndTodayDS = jsonWithWmDS.keyBy(jsonObject -> jsonObject.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    ValueState<String> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("user-pay",
                                String.class));
                    }

                    @Override
                    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>.Context context, Collector<TradePaymentWindowBean> collector) throws Exception {
                        long newPay = 0;
                        long todayPay = 0;
                        String date = jsonObject.getString("callback_time").split(" ")[0];
                        if (valueState.value() == null) {
                            newPay = 1;
                            todayPay = 1;

                        } else {
                            if (!date.equals(valueState.value())) {
                                todayPay = 1;
                            }
                        }
                        valueState.update(date);
                        if (todayPay != 0) {
                            collector.collect(new TradePaymentWindowBean("", "", todayPay, newPay, 0L));
                        }
                    }
                });

     //   newAndTodayDS.print(">>>>beforeWindow");

        SingleOutputStreamOperator<TradePaymentWindowBean> resultDS = newAndTodayDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradePaymentWindowBean>() {
                    @Override
                    public TradePaymentWindowBean reduce(TradePaymentWindowBean tradePaymentWindowBean, TradePaymentWindowBean t1) throws Exception {
                        tradePaymentWindowBean.setPaymentSucNewUserCount(tradePaymentWindowBean.getPaymentSucNewUserCount() + t1.getPaymentSucNewUserCount());
                        tradePaymentWindowBean.setPaymentSucUniqueUserCount(tradePaymentWindowBean.getPaymentSucUniqueUserCount() + t1.getPaymentSucUniqueUserCount());
                        return tradePaymentWindowBean;
                    }
                }, new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>.Context context, Iterable<TradePaymentWindowBean> iterable, Collector<TradePaymentWindowBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        for (TradePaymentWindowBean t1 : iterable) {
                            t1.setStt(stt);
                            t1.setEdt(edt);
                            t1.setTs(System.currentTimeMillis());
                            collector.collect(t1);
                        }
                    }
                });

        resultDS.addSink(MyClickHouseUtil.sink("insert into dws_trade_payment_suc_window " +
                "values(?,?,?,?,?)"));
//        resultDS.print();
        env.execute();


    }
}
