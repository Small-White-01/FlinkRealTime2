package com.flink.realtime.dim;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.TableProcess;
import com.flink.realtime.serializer.MyJsonDeSerializer;
import com.flink.realtime.util.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 问题一：为什么再次消费时，需要换消费者组id，不换时，只能消费者脚本消费，而App消费不到
 *
 * */
public class DimApp {
    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行数，与Kafka分区数一致
        env.setParallelism(1);

        //获取主流，Kafka读取
        String topic="topic_db";
        String groupId="ods_base_db_group3";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.flinkKafkaConsumer(topic, groupId));
        OutputTag<String> outputTag = new OutputTag<String>("nonValid", TypeInformation.of(String.class));
        //转换格式，并过滤不是json的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {


            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(outputTag, s);
                }
            }
        });
        //打印错误信息
        jsonObjDS.getSideOutput(outputTag).print("error>>>>>>");

        //获取配置流信息，由FlinkCDC实时监控Mysql
        MySqlSource<String> mySqlSource = MySqlSource.<String>
                        builder()

                .hostname("hadoop2")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new MyJsonDeSerializer())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlSourceDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");
//        mysqlSourceDS.print();
      //  mysqlSourceDS.print();
        //连接,获取配置信息，筛选出要建的dim表
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = mysqlSourceDS.broadcast(mapStateDescriptor);
        SingleOutputStreamOperator<JSONObject> streamOperator = jsonObjDS.connect(broadcastStream)
                .process(new TableProcessFunction(mapStateDescriptor));
//
//
        streamOperator.map(JSONAware::toJSONString).print();
        streamOperator.addSink(new HbaseSink());
        //执行


        env.execute();




    }
}
