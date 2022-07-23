package com.flink.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//只做去重，不聚合，因为考虑到各种粒度汇总
public class DWDUvDetail {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        String pageTopic="dwd_page_log";
        String group="dwd_traffic_uv_detail";
        FlinkKafkaConsumer<String> kafkaDS = MyKafkaUtil.flinkKafkaConsumer(pageTopic, group);

        env.enableCheckpointing(15000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop2:8020/ck");
        env.getCheckpointConfig().setCheckpointInterval(10000);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        MyKafkaUtil.startOffsetSave(kafkaDS,env);

        OutputTag<String> dirty=new OutputTag<String>("dirty", TypeInformation.of(String.class));
        SingleOutputStreamOperator<JSONObject> jsonObjDS = env.addSource(kafkaDS)
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                        try {
                            JSONObject jsonObject = JSON.parseObject(s);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            context.output(dirty, s);
                        }
                    }
                });



        //1次去重
        SingleOutputStreamOperator<JSONObject> filterDS1 = jsonObjDS.filter(jsonObject -> jsonObject.getJSONObject("page").getString("last_page_id")
                == null);

        SingleOutputStreamOperator<JSONObject> filterDS2 = filterDS1.keyBy(data->
                data.getJSONObject("common").getString("mid"))
                .filter(new RichFilterFunction<JSONObject>() {


            ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> uv = new ValueStateDescriptor<>("uv", String.class);
                uv.enableTimeToLive(
                        StateTtlConfig.newBuilder(Time.days(1))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build()
                );
                state = getRuntimeContext().getState(uv);
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {

                Long ts = jsonObject.getLong("ts");
                String date = DateFormatUtil.toDate(ts);
                if (state.value() == null || !date.equals(state.value())) {
                    state.update(date);
                    return true;
                }


                return false;
            }
        });

        String sinkTopic="dwd_traffic_uv_detail";
        filterDS2.print(">>>>>userVisit");
        filterDS2.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.flinkKafkaProducer(sinkTopic));

        env.execute();
    }
}
