package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TradeTrademarkCategoryUserSpuOrderBean;
import com.flink.realtime.func.DimAsyncJoinFunction;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import com.flink.realtime.util.TimestampUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class DwsTradeTrademarkCategoryUserSpuOrderWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String topic="dwd_trade_order_pre_process";
        String group="dwsTradeTrademarkCategoryUserSpuOrderWindow";
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

        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> trademarkDS = jsonObjLatestDS.map(jsonObject -> {
            long ts = DateFormatUtil.dateTimeToTs(jsonObject.getString("detail_create_time"));
            HashSet<String> orderIdSet = new HashSet<>();
            orderIdSet.add(jsonObject.getString("id"));
            return TradeTrademarkCategoryUserSpuOrderBean.builder()
                    .userId(jsonObject.getString("user_id"))
                    .skuId(jsonObject.getString("sku_id"))
                    .orderAmount(jsonObject.getDouble("split_total_amount"))
                    .orderCount(0L)
                    .orderIdSet(orderIdSet)
                    .ts(ts)
                    .build();
        });
     //   trademarkDS.print(">>>>>beforeJoin");
        // TODO 8. 维度关联
        // 8.1 关联 sku_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                trademarkDS,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_sku_info".toUpperCase()) {

                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) {
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 8.2 关联 spu_info 表
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(
                withSkuInfoStream,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject dimJsonObj) {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 8.3 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.4 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.5 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category2".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 8.6 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncJoinFunction<TradeTrademarkCategoryUserSpuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeTrademarkCategoryUserSpuOrderBean javaBean, JSONObject jsonObj) {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeTrademarkCategoryUserSpuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );


        //设置水位线
        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> withCategory1WithWmStream = withCategory1Stream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<TradeTrademarkCategoryUserSpuOrderBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public long extractTimestamp(TradeTrademarkCategoryUserSpuOrderBean tradeTrademarkCategoryUserSpuOrderBean, long l) {
                        return tradeTrademarkCategoryUserSpuOrderBean.getTs();
                    }
                }));

     //   withCategory1WithWmStream.print(">>>>>afterJoin");

        //开窗聚合
        KeyedStream<TradeTrademarkCategoryUserSpuOrderBean, String> keyStream = withCategory1WithWmStream.keyBy(javaBean -> {
            return javaBean.getTrademarkId() +
                    javaBean.getTrademarkName() +
                    javaBean.getCategory1Id() +
                    javaBean.getCategory1Name() +
                    javaBean.getCategory2Id() +
                    javaBean.getCategory2Name() +
                    javaBean.getCategory3Id() +
                    javaBean.getCategory3Name() +
                    javaBean.getUserId() +
                    javaBean.getSpuId() +
                    javaBean.getSpuName();

        });

        SingleOutputStreamOperator<TradeTrademarkCategoryUserSpuOrderBean> resultDS = keyStream.window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<TradeTrademarkCategoryUserSpuOrderBean>() {
                    @Override
                    public TradeTrademarkCategoryUserSpuOrderBean reduce(TradeTrademarkCategoryUserSpuOrderBean value1, TradeTrademarkCategoryUserSpuOrderBean value2) throws Exception {
                        value1.setOrderCount(value1.getOrderCount() + value2.getOrderCount());
                        value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;

                    }
                }, new ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeTrademarkCategoryUserSpuOrderBean, TradeTrademarkCategoryUserSpuOrderBean, String, TimeWindow>.Context context, Iterable<TradeTrademarkCategoryUserSpuOrderBean> iterable, Collector<TradeTrademarkCategoryUserSpuOrderBean> collector) throws Exception {
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        String stt = DateFormatUtil.toDateTime(start);
                        String edt = DateFormatUtil.toDateTime(end);
                        for (TradeTrademarkCategoryUserSpuOrderBean element : iterable) {
                            element.setStt(stt);
                            element.setEdt(edt);
                            element.setTs(System.currentTimeMillis());
                            long orderCount = element.getOrderIdSet().size();
                            element.setOrderCount(orderCount);
                            collector.collect(element);
                            element.getOrderIdSet().clear();
                        }

                    }
                });

//
        resultDS.addSink(MyClickHouseUtil.sink("insert into dws_trade_trademark_category_user_spu_order_window " +
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        resultDS.print();
//

        env.execute();


    }

}
