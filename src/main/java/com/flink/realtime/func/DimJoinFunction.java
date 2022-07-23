package com.flink.realtime.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {

    String getKey(T t);
    void join(T t, JSONObject jsonObject);

}
