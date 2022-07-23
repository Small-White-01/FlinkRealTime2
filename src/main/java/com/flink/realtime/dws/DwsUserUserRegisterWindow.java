package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.UserRegisterBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserRegisterWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic="dwd_user_register";
        String group="user_register";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));


        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS =
                kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((jsonObject, l) ->
                                //maxwell时间为秒
                                jsonObject.getLong("ts")*1000));


        SingleOutputStreamOperator<UserRegisterBean> resultDS = jsonObjWithWmDS
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<JSONObject, Long, UserRegisterBean>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObject, Long aLong) {
                        return aLong + 1;
                    }

                    @Override
                    public UserRegisterBean getResult(Long aLong) {
                        return new UserRegisterBean("", "", aLong, 0L);
                    }

                    @Override
                    public Long merge(Long aLong, Long acc1) {
                        return null;
                    }
                },
                        new ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<UserRegisterBean, UserRegisterBean, TimeWindow>.Context context, Iterable<UserRegisterBean> iterable, Collector<UserRegisterBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        for (UserRegisterBean userRegisterBean : iterable) {
                            userRegisterBean.setStt(stt);
                            userRegisterBean.setEdt(edt);
                            userRegisterBean.setTs(System.currentTimeMillis());
                            collector.collect(userRegisterBean);
                        }
                    }
                });

        resultDS.addSink(MyClickHouseUtil.sink("insert into dws_user_user_register_window values(?,?,?,?)"));

        env.execute();


    }
}
