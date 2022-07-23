package com.flink.realtime.util;

import com.flink.realtime.annotation.TransientSink;
import com.flink.realtime.bean.ClickHouseConstants;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyClickHouseUtil {

    public static <T> SinkFunction<T> sink(String sql){

        return JdbcSink.sink(sql, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                Class<?> aClass = t.getClass();
                Field[] declaredFields = aClass.getDeclaredFields();
                int offset=0;
                for (int i = 0; i < declaredFields.length; i++) {

                    Field declaredField = declaredFields[i];
                    if(declaredField.getAnnotation(TransientSink.class)!=null){
                        offset++;
                        continue;
                    }
                    declaredField.setAccessible(true);
                    try {
                        preparedStatement.setObject(i+1-offset,declaredField.get(t));
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                }


            }
        }, JdbcExecutionOptions.builder()
                        .withBatchSize(5)
                        .withBatchIntervalMs(1000)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(ClickHouseConstants.CLICKHOUSE_DRIVER)
                .withUrl(ClickHouseConstants.CLICKHOUSE_URL)
                    .withUsername(ClickHouseConstants.CLICKHOUSE_USERNAME)
                    .withPassword(ClickHouseConstants.CLICKHOUSE_PASSWORD)
                .build());


    }


}
