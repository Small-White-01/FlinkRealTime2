package com.flink.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.util.DateFormatUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdLog {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，与kafka分区数一致
        env.setParallelism(1);

        String topic="topic_log";
        String group="ods_base_log_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, group));

        OutputTag<String> dirtyLog=new OutputTag<String>("dirty"){};
        //过滤脏数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyLog, s);
                }
            }
        });

        jsonObjDS.getSideOutput(dirtyLog).print("dirty>>>");
        //新老用户校验

        KeyedStream<JSONObject, String> keyStream = jsonObjDS.keyBy(data -> data.
                getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> jsonLogDS = keyStream
                .map(new RichMapFunction<JSONObject, JSONObject>() {

            ValueState<String> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<String>("isNew",
                        String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                JSONObject common = jsonObject.getJSONObject("common");
                String isNew = (String) common.getString("is_new");
                //判断isNew字段，如果为1，可能为老用户
                Long ts = jsonObject.getLong("ts");
                String date = DateFormatUtil.toDate(ts);
                if ("1".equals(isNew)) {

                    if (state.value() == null || state.value().equals(date)) {
                        //首次访问   //首日访问，不需更改
                        state.update(date);
                    } else {
                        common.put("is_new", 0);
                    }
                } else if (state.value() == null) {
                    //旧访客isNew为0，状态为空时,应对首日
                    Long yesterday = ts - 3600 * 24 * 1000;
                    String s = DateFormatUtil.toDate(yesterday);
                    state.update(s);
                }


                return jsonObject;
            }
        });


        //分流,需要从测试的日志中，找出各类型的日志
        //启动日志
        OutputTag<String> start=new OutputTag<String>("start",
                TypeInformation.of(String.class)){};
//        //页面浏览
//        OutputTag<String> page=new OutputTag<String>("page"){};
        //
        OutputTag<String> displays=new OutputTag<String>("displays",
                TypeInformation.of(String.class)){};

        OutputTag<String> actions=new OutputTag<String>("actions",
                TypeInformation.of(String.class)){};

        OutputTag<String> error=new OutputTag<String>("error",
                TypeInformation.of(String.class)){};

        SingleOutputStreamOperator<String> sideOutDS = jsonLogDS
                .process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject,
                                       ProcessFunction<JSONObject, String>.Context context,
                                       Collector<String> collector) throws Exception {

                JSONObject start1 = jsonObject.getJSONObject("start");
                String error1 = jsonObject.getString("error");
                if (error1 != null && !"".equals(error1)) {
                    context.output(error, jsonObject.toJSONString());
                }
                if (start1 != null) {
                    context.output(start, start1.toJSONString());
                } else {
                    JSONObject page = jsonObject.getJSONObject("page");
                    String page_id = page.getString("page_id");
                    String ts = jsonObject.getString("ts");
                    JSONArray displays1 = jsonObject.getJSONArray("displays");
                    if (displays1 != null && displays1.size() > 0) {
                        for (int i = 0; i < displays1.size(); i++) {
                            JSONObject displayJson = displays1.getJSONObject(i);
                            displayJson.put("page",page_id);
                            displayJson.put("ts",ts);

                            context.output(displays, displayJson.toJSONString());
                        }
                    }
                    JSONArray actions1 = jsonObject.getJSONArray("actions");
                    if (actions1 != null && actions1.size() > 0) {
                        for (int i = 0; i < actions1.size(); i++) {
                            JSONObject actionJson = actions1.getJSONObject(i);

                            actionJson.put("page",page_id);
                            actionJson.put("ts",ts);
                            context.output(actions, actionJson.toJSONString());
                        }
                    }
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    String json = jsonObject.toJSONString();
                    collector.collect(json);

                }
            }
        });

        //sink

//        sideOutDS.print();
        String startTopic="dwd_start_log";
        String pageTopic="dwd_page_log";
        String actionTopic="dwd_action_log";
        String displayTopic="dwd_display_log";
        String errorTopic="dwd_error_log";

        sideOutDS.getSideOutput(start).addSink(MyKafkaUtil.flinkKafkaProducer(startTopic));
        sideOutDS.getSideOutput(error).addSink(MyKafkaUtil.flinkKafkaProducer(errorTopic));
        sideOutDS.getSideOutput(actions).addSink(MyKafkaUtil.flinkKafkaProducer(actionTopic));
        sideOutDS.getSideOutput(displays).addSink(MyKafkaUtil.flinkKafkaProducer(displayTopic));
        sideOutDS.addSink(MyKafkaUtil.flinkKafkaProducer(pageTopic));//页面浏览
//        sideOutDS.print();
        //执行
        env.execute();
    }
}
