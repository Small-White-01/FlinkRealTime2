package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.UserSevenDayLastDayLoginBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取kafka，pageLog
        String page="dwd_page_log";
        String group="dwsUserUserLoginWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(page, group));
        //过滤，水位线，keyby
        SingleOutputStreamOperator<JSONObject> pageWithWaterMarkDS = kafkaDS.map(JSON::parseObject).
        filter(jsonObject -> jsonObject.getJSONObject("page").getString("last_page_id")==null&&
                jsonObject.getJSONObject("common").getString("uid")!=null)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((jsonObject, l) -> jsonObject.getLong("ts")));
        KeyedStream<JSONObject, String> midDS = pageWithWaterMarkDS
                .keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("uid"));

        //状态编程
        SingleOutputStreamOperator<UserSevenDayLastDayLoginBean> userSevenAndOneDS =
                midDS.process(new KeyedProcessFunction<String, JSONObject, UserSevenDayLastDayLoginBean>() {

            ValueState<String> dateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> date = new ValueStateDescriptor<>("date", String.class);
                dateState = getRuntimeContext().getState(date);

            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, UserSevenDayLastDayLoginBean>
                    .Context context, Collector<UserSevenDayLastDayLoginBean> collector) throws Exception {
                Long ts = jsonObject.getLong("ts");
                String today = DateFormatUtil.toDate(ts);
                long lastSeven = 0L;
                long lastOne = 0L;
                if (dateState.value() == null ) {
                    lastOne += 1;
                    dateState.update(today);
                }else if(!today.equals(dateState.value())){
                    long ts2 = DateFormatUtil.dateToTs(dateState.value());
                    long ts3 = DateFormatUtil.dateToTs(today);
                    long last = (ts3 - ts2) / (24 * 3600 * 1000);
                    if (last > 7) {
                        lastSeven += 1L;
                    }
                    lastOne += 1;
                    dateState.update(today);
                }
                if(lastOne!=0)
                collector.collect(new UserSevenDayLastDayLoginBean("", "", lastSeven, lastOne, ts));

            }
        });


        //窗口汇聚

        SingleOutputStreamOperator<UserSevenDayLastDayLoginBean> resultDS = userSevenAndOneDS.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<UserSevenDayLastDayLoginBean>() {
                    @Override
                    public UserSevenDayLastDayLoginBean reduce(UserSevenDayLastDayLoginBean u1, UserSevenDayLastDayLoginBean u2) throws Exception {
                        u1.setBackCt(u1.getBackCt() + u2.getBackCt());
                        u1.setUuCt(u1.getUuCt() + u2.getUuCt());
                        return u1;
                    }
                }, new ProcessAllWindowFunction<UserSevenDayLastDayLoginBean, UserSevenDayLastDayLoginBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserSevenDayLastDayLoginBean, UserSevenDayLastDayLoginBean, TimeWindow>.Context context, Iterable<UserSevenDayLastDayLoginBean> iterable, Collector<UserSevenDayLastDayLoginBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        for (UserSevenDayLastDayLoginBean u1 : iterable) {
                            u1.setStt(stt);
                            u1.setEdt(edt);
                            collector.collect(u1);
                        }
                    }
                });

        //sink
      // resultDS.print();
        resultDS.addSink(MyClickHouseUtil.sink("insert into dws_user_user_login_window values(?,?,?,?,?)"));
        env.execute();


    }
}
