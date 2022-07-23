package com.flink.realtime.dws;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TrafficHomeDetailPageViewBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class DwsHomeAndDetailStat {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        
        String page="dwd_page_log";
        String topic="dwsHomeAndDetailStat";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(page, topic));

        SingleOutputStreamOperator<JSONObject> homeAndGoodDetailDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            final List<String> targetList = Arrays.asList("good_detail", "home");

            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(s);
                JSONObject page = jsonObject.getJSONObject("page");
                String pageId = page.getString("page_id");
                if (targetList.contains(pageId)) {
                    collector.collect(jsonObject);
                }

            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((jsonObject, l) -> jsonObject.getLong("ts")));


        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailPageViewBeanDS = homeAndGoodDetailDS
                .keyBy(data->data.getJSONObject("common").getString("mid")).flatMap(
                        new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {

            ValueState<String> homeState;
            ValueState<String> detailState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> homeDes = new ValueStateDescriptor<>("home", String.class);
                homeDes.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                homeState = getRuntimeContext().getState(homeDes);
                ValueStateDescriptor<String> detailDes = new ValueStateDescriptor<>("detail", String.class);
                detailDes.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build());
                detailState = getRuntimeContext().getState(detailDes);

            }

            @Override
            public void flatMap(JSONObject jsonObject, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                JSONObject page1 = jsonObject.getJSONObject("page");
                String pageId = page1.getString("page_id");
                Long ts = jsonObject.getLong("ts");
                String date = DateFormatUtil.toDate(ts);
                long gd_dt = 0;
                long ho_dt = 0;
                if ("good_detail".equals(pageId)) {
                    if (detailState.value() == null || !date.equals(detailState.value())) {
                        detailState.update(date);
                        gd_dt = 1;
                    }
                } else {
                    if (homeState.value() == null || !date.equals(homeState.value())) {
                        homeState.update(date);
                        ho_dt = 1;
                    }
                }
                if (gd_dt != 0 || ho_dt != 0) {
                    long l = System.currentTimeMillis();
                    //ts用作clickhouse版本号更新，所以这System.currentTimeMillis()也可以
                    collector.collect(new TrafficHomeDetailPageViewBean("", "", ho_dt, gd_dt, l));
                }
            }
        });


        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> homeDetailWithWmDS =
                trafficHomeDetailPageViewBeanDS.windowAll(
                        TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(3)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean t1, TrafficHomeDetailPageViewBean t2) throws Exception {
                        t1.setHomeUvCt(t1.getHomeUvCt() + t2.getHomeUvCt());
                        t1.setGoodDetailUvCt(t1.getGoodDetailUvCt() + t2.getGoodDetailUvCt());
                        return t1;
                    }
                }, new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.toDateTime(window.getStart());
                        String edt = DateFormatUtil.toDateTime(window.getEnd());
                        for (TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean:iterable) {
                            trafficHomeDetailPageViewBean.setStt(stt);
                            trafficHomeDetailPageViewBean.setEdt(edt);
                            collector.collect(trafficHomeDetailPageViewBean);
                        }
                    }
                });


        homeDetailWithWmDS.addSink(MyClickHouseUtil.sink("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

       // homeDetailWithWmDS.print();
        env.execute();

    }
}
