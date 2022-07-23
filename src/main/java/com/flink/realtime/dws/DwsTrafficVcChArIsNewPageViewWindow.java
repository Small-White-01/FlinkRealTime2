package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TrafficPageViewBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow {
    public static void main(String[] args) throws Exception {

        //获取环境，并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取kafka
        String page="dwd_page_log";
        String uvDetail="dwd_traffic_uv_detail";
        String ujDetail="dwd_traffic_uj_detail";
        String group="dwsTrafficVcChArIsNewPageViewWindow";
        SingleOutputStreamOperator<TrafficPageViewBean> pageTrafficPageViewBean =
                env.addSource(MyKafkaUtil.flinkKafkaConsumer(page, group))
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    JSONObject common = jsonObject.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    return new TrafficPageViewBean("", "", vc, ch, ar, is_new,
                            0L,
                            jsonObject.getJSONObject("page").getString("last_page_id") == null ? 1L : 0L,
                            1L,
                            jsonObject.getJSONObject("page").getLong("during_time"),
                            0L,
                            jsonObject.getLong("ts"));
                });

//        pageTrafficPageViewBean.print("pageTrafficPageViewBean>>>");
        SingleOutputStreamOperator<TrafficPageViewBean> uvTrafficPageViewBean = env.addSource(MyKafkaUtil.flinkKafkaConsumer(uvDetail, group))
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    JSONObject common = jsonObject.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    return new TrafficPageViewBean("", "", vc, ch, ar, is_new,
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            jsonObject.getLong("ts"));
                }).returns(TrafficPageViewBean.class);
//        uvTrafficPageViewBean.print("uvTrafficPageViewBean>>");

        SingleOutputStreamOperator<TrafficPageViewBean> ujTrafficPageViewBean = env.addSource(MyKafkaUtil.flinkKafkaConsumer(ujDetail, group))
                .map(json -> {
                    JSONObject jsonObject = JSON.parseObject(json);
                    JSONObject common = jsonObject.getJSONObject("common");
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String is_new = common.getString("is_new");

                    return new TrafficPageViewBean("", "", vc, ch, ar, is_new,
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            jsonObject.getLong("ts"));
                }).returns(TrafficPageViewBean.class);


        SingleOutputStreamOperator<TrafficPageViewBean> unionDS = pageTrafficPageViewBean.union(uvTrafficPageViewBean, ujTrafficPageViewBean)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(12))
                        .withTimestampAssigner((trafficPageViewBean, l) -> trafficPageViewBean.getTs()));

        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyStream = unionDS.keyBy(new KeySelector<TrafficPageViewBean, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(TrafficPageViewBean trafficPageViewBean) throws Exception {
                return Tuple4.of(trafficPageViewBean.getVc(),
                        trafficPageViewBean.getCh(),
                        trafficPageViewBean.getAr(),
                        trafficPageViewBean.getIsNew());
            }
        });


        SingleOutputStreamOperator<TrafficPageViewBean> reduceDS =
                keyStream
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        //.allowedLateness(Time.milliseconds(10000))
                .reduce(new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean t1, TrafficPageViewBean t2) throws Exception {
                        t1.setUvCt(t1.getUvCt() + t2.getUvCt());
                        t1.setSvCt(t1.getSvCt() + t2.getSvCt());
                        t1.setPvCt(t1.getPvCt() + t2.getPvCt());
                        t1.setUjCt(t1.getUjCt() + t2.getUjCt());
                        t1.setDurSum(t1.getDurSum() + t2.getDurSum());
                        return t1;
                    }
                }, new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        for (TrafficPageViewBean trafficPageViewBean :iterable) {

                            trafficPageViewBean.setStt(stt);
                            trafficPageViewBean.setEdt(edt);
                            collector.collect(trafficPageViewBean);
                        }
                    }
                });

//        reduceDS.print();

//
        reduceDS.addSink(MyClickHouseUtil.sink("insert into dws_traffic_vc_ch_ar_is_new_page_view_window " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?)"));


        env.execute();


    }
}
