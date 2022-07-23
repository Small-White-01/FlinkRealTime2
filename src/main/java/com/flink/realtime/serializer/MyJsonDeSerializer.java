package com.flink.realtime.serializer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyJsonDeSerializer implements DebeziumDeserializationSchema<String> {

    /**
     * SourceRecord{sourcePartition={server=mysql_binlog_source},
     * sourceOffset={transaction_id=null, ts_sec=1656692626, file=mysql-bin.000008,
     * pos=6449970, row=1, server_id=1, event=2}}
     * ConnectRecord{topic='mysql_binlog_source.gmall_config.table_process', kafkaPartition=null,
     * key=Struct{source_table=aaa}, keySchema=Schema{mysql_binlog_source.gmall_config.table_process.Key:STRUCT},
     * value=Struct{before=Struct{source_table=aaa,sink_table=},
     * after=Struct{source_table=aaa,sink_table=bb},
     * source=Struct{version=1.5.4.Final,connector=mysql,
     * name=mysql_binlog_source,ts_ms=1656692626000,db=gmall_config,table=table_process,server_id=1,file=mysql-bin.000008,
     * pos=6450143,row=0},op=u,ts_ms=1656692626102}, valueSchema=Schema{mysql_binlog_source.gmall_config.table_process.Envelope:STRUCT},
     *  timestamp=null, headers=ConnectHeaders(headers=)}
     *
     *
     *  value=Struct{before=Struct{source_table=aaa,sink_table=},
     *      after=Struct{source_table=aaa,sink_table=bb},
     *      source=Struct{version=1.5.4.Final,connector=mysql,
     *      name=mysql_binlog_source,ts_ms=1656692626000,db=gmall_config,table=table_process,server_id=1,
     *      file=mysql-bin.000008,
     *      pos=6450143,row=0},op=u,ts_ms=1656692626102}
     * @param sourceRecord
     * @param collector
     * @throws Exception
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Struct value =(Struct) sourceRecord.value();
        Struct source = value.getStruct("source");
        String table = source.getString("table");
        Struct before = value.getStruct("before");
        Struct after = value.getStruct("after");
        String op = value.getString("op");

        JSONObject result=new JSONObject();
        JSONObject beforeJson=new JSONObject();
        JSONObject afterJson=new JSONObject();
        switch (op){
            case "r":{

                result.put("type","bootstrap-insert");
                buildJson(after,afterJson);
                break;
            }
            case "c":{

                result.put("type","insert");
                buildJson(after,afterJson);
                break;
            }
            case "u":{
                result.put("type","update");
                buildJson(before,beforeJson);
                buildJson(after,afterJson);
                break;
            }
            case "d":{
                result.put("type","delete");
                buildJson(before,beforeJson);
                break;
            }
        }
        result.put("table",table);
        result.put("before",beforeJson);
        result.put("after",afterJson);
        collector.collect(result.toJSONString());

    }

    private void buildJson(Struct struct, JSONObject jsonObject) {
        List<Field> fields = struct.schema().fields();
        if(fields!=null&&fields.size()>0){
            for (Field field:fields){
                jsonObject.put(field.name(),struct.get(field));
            }
        }
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
