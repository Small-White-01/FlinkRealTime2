package com.flink.realtime.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class MyKafkaUtil {

    public static FlinkKafkaConsumer<String> flinkKafkaConsumer(String topic,String group,boolean fromBeginning){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop2:9092,hadoop3:9092,hadoop4:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,group);

        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                        if (consumerRecord == null || consumerRecord.value() == null) {
                            return "";
                        }
                        return new String(consumerRecord.value(), StandardCharsets.UTF_8);
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                }, properties);
        if(fromBeginning) {
           // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            flinkKafkaConsumer.setStartFromEarliest();
        }
        return flinkKafkaConsumer;
    }
    public static FlinkKafkaConsumer<String> flinkKafkaConsumer(String topic,String group){
        return flinkKafkaConsumer(topic, group,false);
    }

    public static void startOffsetSave(FlinkKafkaConsumer<String> flinkKafkaConsumer,
                                       StreamExecutionEnvironment env){

        if(!env.getCheckpointConfig().isCheckpointingEnabled()){
            throw new RuntimeException("must set checkpoint enabled");
        }
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

    }
//    static Properties producerProperties=new Properties();
//    static {
//        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop2:9092,hadoop3:9092,hadoop4:9092");
//    }
    public static FlinkKafkaProducer<String> flinkKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>("hadoop2:9092,hadoop3:9092,hadoop4:9092",
                topic,new SimpleStringSchema());
    }

    public static String getKafkaDDLConnectors(String topic,String groupId){
        return  "WITH( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '"+topic+"', " +
                "  'properties.bootstrap.servers' = 'hadoop2:9092,hadoop3:9092,hadoop4:9092', " +
                "  'properties.group.id' = '"+groupId+"', " +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'format' = 'json' )";
    }
    //当sql leftjoin时用到
    public static String getUpdateKafkaDDLConnectors(String topic){
        return  "WITH( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = '"+topic+"', " +
                "  'properties.bootstrap.servers' = 'hadoop2:9092,hadoop3:9092,hadoop4:9092', " +
                "'key.format' = 'json'," +
                "'value.format' = 'json'" +
                ")";
    }
    public static String getTopicDB(){
        return "create table topic_db( " +
                "   `database` STRING, " +
                "   `table` STRING, " +
                "   `type` STRING, " +
                "   `ts` STRING, " +
                "   `data` Map<STRING,STRING>, " +
                "   `old` Map<STRING,STRING> , " +
                "   `proc_time` as PROCTIME()" +
                ") ";
    }
}
