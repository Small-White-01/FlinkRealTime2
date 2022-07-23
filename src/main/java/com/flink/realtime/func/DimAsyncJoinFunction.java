package com.flink.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.util.DIMUtil;
import com.flink.realtime.util.PhoenixUtil;
import com.flink.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class DimAsyncJoinFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {

    Connection connection;
    ThreadPoolExecutor threadPoolExecutor;
    String tableName;

    public DimAsyncJoinFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        connection= PhoenixUtil.getConnection();
        threadPoolExecutor=ThreadPoolUtil.getThreadPoolExecutor();

    }


    public abstract String getKey(T t);
    public abstract void join(T t,JSONObject jsonObject);

    @Override
    public void asyncInvoke(T t, ResultFuture<T> resultFuture) throws Exception {
        threadPoolExecutor.execute(()-> {
                    //获得维表数据
                    try {
                        JSONObject dimData = DIMUtil.getDimData(connection, tableName, getKey(t));


                        //关联join
                        join(t, dimData);

                        //输出
                        resultFuture.complete(Collections.singletonList(t));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            );
    }

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }
}
