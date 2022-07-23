package com.flink.realtime.dwd;


import akka.pattern.Patterns;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 用户跳出会话的次数 last_page_id为null为跳出
 * 两次紧挨着的last_page_id为null
 * 针对每一个mid用户做统计，keyby
 * 方案一：会话窗口
 *    基于会话，统计会话内只有一条数据时收集，即为跳出。
 *    优点：基于水位线，保证数据不乱序
 *    缺点：当一个会话窗口有两次进入后跳出，导致此窗口数据丢失
 * 方案二：状态编程
 *    使用valueState,第一条last_page_id为null时记录state，不输出，设置定时
 *    1.超时即算为跳出，定时器触发输出并清空状态 2.定时器内两次last_page_id为null
 *    情况一：定时器内第二条如果last_page_id不为null，表明不属于跳出，清空状态，删除定时器
 *    情况二：定时器内第二条如果last_page_id为null，输出状态内数据，并记录自己，重置定时器
 *    优点：一定时间内两次last_page_id为null都可以统计
 *    缺点：无法使用水位线，若第三条last_page_id为null抢先来到，导致第一条输出，即无法处理乱序
 * 方案三：CEP
 *    整合前两个优点
 */
public class Dwd_traffic_uj_detail {


    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setParallelism(1);

        env.enableCheckpointing(15000);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        String topic="dwd_page_log";
        String group="dwd_traffic_uj_detail_grp";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = MyKafkaUtil.flinkKafkaConsumer(topic, group);
        MyKafkaUtil.startOffsetSave(flinkKafkaConsumer,env);
        DataStreamSource<String> kafkaDS = env.addSource(flinkKafkaConsumer);

        SingleOutputStreamOperator<JSONObject> waterMarkDS = kafkaDS.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((jsonObject, l) -> jsonObject.getLong("ts")));


        KeyedStream<JSONObject, String> midKeyStream = waterMarkDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));


        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("next")
                .where(new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
//                .times(2)
//                .consecutive()
                .within(Time.seconds(10));


        OutputTag<JSONObject> timeout=new OutputTag<JSONObject>("timeout", TypeInformation.of(JSONObject.class));
        SingleOutputStreamOperator<JSONObject> patternStream = CEP.pattern(midKeyStream, pattern)
                .select(timeout, new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {
                        return map.get("first").get(0);
                    }
                }, new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {
                        return map.get("first").get(0);
                    }
                });
        DataStream<JSONObject> sideOutput = patternStream.getSideOutput(timeout);

        DataStream<JSONObject> ujStream = patternStream.union(sideOutput);

       // sideOutput.print(">>>>timeout");
        ujStream.print(">>>>userJoin");
        String sinkTopic="dwd_traffic_uj_detail";
        ujStream.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.flinkKafkaProducer(sinkTopic));

        env.execute();




    }





}
