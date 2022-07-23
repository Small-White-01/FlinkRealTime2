package com.flink.realtime.dwd;

import com.flink.realtime.MyBloomFilter;
import com.flink.realtime.util.MyKafkaUtil;
import jdk.nashorn.internal.codegen.types.Type;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;

public class DwdTestUvBloomFilter {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String test="test";
        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.flinkKafkaConsumer(test, "test-group",true));

        SingleOutputStreamOperator<String> objectSingleOutputStreamOperator =
                streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split("");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        SingleOutputStreamOperator<String> filterDS = objectSingleOutputStreamOperator.filter(new RichFilterFunction<String>() {

            MyBloomFilter myBloomFilter;
            Map map;

            @Override
            public void open(Configuration parameters) throws Exception {

                myBloomFilter = new MyBloomFilter(10000);
            }

            @Override
            public boolean filter(String s) throws Exception {
                if (myBloomFilter.exist(s)) {
                    myBloomFilter.del(s);
                    return false;
                }
                myBloomFilter.put(s);
                return true;

            }
        });

        filterDS.print();
        env.execute();
    }
}
