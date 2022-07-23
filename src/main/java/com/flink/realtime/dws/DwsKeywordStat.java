package com.flink.realtime.dws;

import com.flink.realtime.bean.KeywordBean;
import com.flink.realtime.func.SplitFunction;
import com.flink.realtime.util.MyClickHouseUtil;
import com.flink.realtime.util.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsKeywordStat {

    public static void main(String[] args) throws Exception {

        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //
        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env);
        String topic="dwd_page_log";
        String group="dws_traffic_keyword_stat";
        tableEnv.executeSql("" +
                "create table pageLog(\n" +
                "  `page` MAP<STRING,STRING>,\n" +
                "  `ts` BIGINT,\n" +
                "  `time_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "   WATERMARK FOR time_ltz AS time_ltz - INTERVAL '5' SECOND\n" +
                ")"+ MyKafkaUtil.getKafkaDDLConnectors(topic,group));


        tableEnv.createTemporaryFunction("my_split", SplitFunction.class);
//
        Table keyword = tableEnv.sqlQuery(
                "select\n" +
                        "   page['last_page_id'] source,\n" +
                        "   page['item'] keyword,\n" +
                        "   ts,\n" +
                        "   time_ltz\n" +
                        "from pageLog\n" +
                        "where page['item_type']='keyword'\n" +
                        " and page['last_page_id']='search'");
        tableEnv.createTemporaryView("page_keyword",keyword);
        Table table = tableEnv.sqlQuery("" +
                "select\n" +
                "  source,\n" +
                "  word,\n" +
                "  time_ltz\n" +


                "from page_keyword,LATERAL TABLE(my_split(keyword)) ");

        tableEnv.createTemporaryView("split_word",table);
//        tableEnv.toChangelogStream(table).print();
        Table window = tableEnv.sqlQuery("select\n" +
                "  source,\n" +
                "  DATE_FORMAT(TUMBLE_START(time_ltz,INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  DATE_FORMAT(TUMBLE_END(time_ltz,INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "  word keyword,\n" +
                "  count(*) keyword_count,\n" +
                "  UNIX_TIMESTAMP(DATE_FORMAT(TUMBLE_START(time_ltz,INTERVAL '5' SECOND),'yyyy-MM-dd HH:mm:ss')) ts " +
                "from split_word \n" +
                "group by TUMBLE(time_ltz,INTERVAL '5' SECOND) ,source,word");
        tableEnv.createTemporaryView("window_count",window);

     //  tableEnv.sqlQuery("select * from window_count").execute().print();

        DataStream<KeywordBean> keywordBeanDataStream = tableEnv.toAppendStream(window, KeywordBean.class);
        keywordBeanDataStream.addSink(MyClickHouseUtil.sink("insert into dws_traffic_source_keyword_page_view_window " +
                "values(?,?,?,?,?,?)"));

        env.execute();


    }

}
