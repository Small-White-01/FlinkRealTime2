package com.flink.realtime.dws;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TradeTmCategoryUserBean;
import com.flink.realtime.func.DimAsyncJoinFunction;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.TimestampUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class DwsTradeRefundWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        String topic="dwd_trade_order_refund";
        String group="dwsTradeRefundWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));
        SingleOutputStreamOperator<JSONObject> filterearliestDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        if (!"".equals(s)) {
                            collector.collect(JSONObject.parseObject(s));
                        }
                    }
                }).keyBy(jsonObject -> jsonObject.getString("id"))
                .filter(new RichFilterFunction<JSONObject>() {

                    ValueState<JSONObject> earliestState;


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<JSONObject> earliest = new ValueStateDescriptor<>("earliest", JSONObject.class);
                        earliest.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        earliestState = getRuntimeContext()
                                .getState(earliest);
                        
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        if(earliestState.value()==null){
                            earliestState.update(jsonObject);
                            return true;
                        }
                        return false;
                    }
                });

        SingleOutputStreamOperator<TradeTmCategoryUserBean> refundDS = filterearliestDS.map(jsonObject -> {
            String refund_time = jsonObject.getString("create_time");
            long ts = DateFormatUtil.dateTimeToTs(refund_time);
            return TradeTmCategoryUserBean.builder()
                    .userId(jsonObject.getString("user_id"))
                    .skuId(jsonObject.getString("sku_id"))
                    .refundCount(1L)
                    .ts(ts)
                    .build();
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeTmCategoryUserBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((tradeTmCategoryUserBean, l) -> tradeTmCategoryUserBean.getTs()));

        SingleOutputStreamOperator<TradeTmCategoryUserBean> resultDS = refundDS.keyBy(new KeySelector<TradeTmCategoryUserBean, String>() {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) throws Exception {
                        return bean.getUserId()+ bean.getSkuId();
//                                +bean.getTrademarkId() + bean.getTrademarkName()
//                                + bean.getCategory3Id() + bean.getCategory3Name()
//                                + bean.getCategory2Id() + bean.getCategory2Name()
//                                + bean.getCategory1Id() + bean.getCategory1Name();
                    }
                }).window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTmCategoryUserBean>() {
                    @Override
                    public TradeTmCategoryUserBean reduce(TradeTmCategoryUserBean bean, TradeTmCategoryUserBean t1) throws Exception {
                        bean.setRefundCount(bean.getRefundCount() + t1.getRefundCount());
                        return bean;
                    }
                }, new ProcessWindowFunction<TradeTmCategoryUserBean, TradeTmCategoryUserBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeTmCategoryUserBean, TradeTmCategoryUserBean, String, TimeWindow>.Context context, Iterable<TradeTmCategoryUserBean> iterable, Collector<TradeTmCategoryUserBean> collector) throws Exception {
                        TradeTmCategoryUserBean userBean = iterable.iterator().next();
                        long start = context.window().getStart();
                        long end = context.window().getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        userBean.setStt(stt);
                        userBean.setEdt(edt);
                        userBean.setTs(System.currentTimeMillis());
                        collector.collect(userBean);
                    }
                });


        SingleOutputStreamOperator<TradeTmCategoryUserBean> withSkuDS =
                AsyncDataStream.unorderedWait(resultDS, new DimAsyncJoinFunction<TradeTmCategoryUserBean>(
                        "DIM_SKU_INFO") {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) {
                        return bean.getSkuId();
                    }

                    @Override
                    public void join(TradeTmCategoryUserBean bean, JSONObject jsonObject) {
                        bean.setTrademarkId(jsonObject.getString("TM_ID"));
                        bean.setCategory3Id(jsonObject.getString("CATEGORY3_ID"));
                    }
                },
                60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTmCategoryUserBean> withTmDS =
                AsyncDataStream.unorderedWait(withSkuDS,
                        new DimAsyncJoinFunction<TradeTmCategoryUserBean>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) {
                        return bean.getTrademarkId();
                    }

                    @Override
                    public void join(TradeTmCategoryUserBean bean, JSONObject jsonObject) {
                        bean.setTrademarkName(jsonObject.getString("TM_NAME"));
                    }
                },
                60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTmCategoryUserBean> withCategory3DS = AsyncDataStream.unorderedWait(withTmDS,
                new DimAsyncJoinFunction<TradeTmCategoryUserBean>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) {
                        return bean.getCategory3Id();
                    }

                    @Override
                    public void join(TradeTmCategoryUserBean bean, JSONObject jsonObject) {
                        bean.setCategory3Name(jsonObject.getString("NAME"));
                        bean.setCategory2Id(jsonObject.getString("CATEGORY2_ID"));
                    }
                },
                60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTmCategoryUserBean> withCategory2DS = AsyncDataStream.
                unorderedWait(withCategory3DS,
                        new DimAsyncJoinFunction<TradeTmCategoryUserBean>("DIM_BASE_CATEGORY2") {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) {
                        return bean.getCategory2Id();
                    }

                    @Override
                    public void join(TradeTmCategoryUserBean bean, JSONObject jsonObject) {
                        bean.setCategory2Name(jsonObject.getString("NAME"));
                        bean.setCategory1Id(jsonObject.getString("CATEGORY1_ID"));
                    }
                },
                60, TimeUnit.SECONDS);
        SingleOutputStreamOperator<TradeTmCategoryUserBean> withCategory1DS =
                AsyncDataStream.unorderedWait(withCategory2DS,
                        new DimAsyncJoinFunction<TradeTmCategoryUserBean>("DIM_BASE_CATEGORY1") {
                    @Override
                    public String getKey(TradeTmCategoryUserBean bean) {
                        return bean.getCategory1Id();
                    }

                    @Override
                    public void join(TradeTmCategoryUserBean bean, JSONObject jsonObject) {
                        bean.setCategory1Name(jsonObject.getString("NAME"));
                    }
                },
                60, TimeUnit.SECONDS);
//
//        withCategory1DS.print();

        withCategory1DS.addSink(MyClickHouseUtil.sink("insert into dws_trade_trademark_category_user_refund_window " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        env.execute();


    }
}
