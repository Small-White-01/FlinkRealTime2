package com.flink.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.CartAddUuBean;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseUtil;

import java.time.Duration;

public class DwsTradeCartCtWindow {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String topic="dwd_trade_cart_add";
        String group="dwsTradeCartCtWindow";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));

        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = kafkaDS
                .map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts")*1000;
                            }
                        }));


        // TODO 6. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(r -> r.getString("user_id"));

        // TODO 7. 筛选加购独立用户
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastCartAddDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("last_cart_add_dt", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                        Time.days(1)).setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                        lastCartAddDt = getRuntimeContext().getState(
                                valueStateDescriptor
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastCartAdd = lastCartAddDt.value();

                        String cartAddDt = DateFormatUtil.toDate(jsonObj.getLong("ts"));
                        if (lastCartAdd == null || !lastCartAdd.equals(cartAddDt)) {
                            lastCartAddDt.update(cartAddDt);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // TODO 8. 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = filteredStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObj, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        String stt = DateFormatUtil.toDateTime(window.getStart());
                        String edt = DateFormatUtil.toDateTime(window.getEnd());
                        for (Long value : values) {
                            CartAddUuBean cartAddUuBean = new CartAddUuBean(
                                    stt,
                                    edt,
                                    value,
                                    System.currentTimeMillis()
                            );
                            out.collect(cartAddUuBean);
                        }
                    }
                }
        );

        // TODO 10. 写入到 OLAP 数据库
        SinkFunction<CartAddUuBean> jdbcSink = MyClickHouseUtil.sink(
                "insert into dws_trade_cart_add_uu_window values(?,?,?,?)"
        );
        aggregateDS.addSink(jdbcSink);

        env.execute();
    }
}




