package com.flink.realtime.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.realtime.bean.HbaseConstants;
import redis.clients.jedis.Jedis;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class DIMUtil {


    public static JSONObject getDimData(Connection connection,String table, String id) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Jedis jedis = JedisUtil.getResource();
        String redisKey="DIM:"+table+":"+id;
        if(jedis.exists(redisKey)){
            String s = jedis.get(redisKey);
            jedis.expire(redisKey,24*60*60);
            jedis.close();
            return JSON.parseObject(s);
        }

        String sql="select * from "+ HbaseConstants.DATABASE+"."+table+" where id='"+id+"'";
        List<JSONObject> jsonObjects = JdbcUtil.query(connection, sql, JSONObject.class, false);
        JSONObject jsonObject=null;
        try {
            jsonObject  = jsonObjects.get(0);
        }catch (Exception e){
            throw new RuntimeException(e.getMessage()+"："+redisKey+"在hbase查不到数据");
        }
        jedis.setex(redisKey,24*60*60,jsonObject.toJSONString());
        jedis.close();

        return jsonObject;
    }

    public static void delKey(String table,String id){
        Jedis jedis = JedisUtil.getResource();
        String redisKey="DIM:"+table+":"+id;
        if(jedis.exists(redisKey)){
            jedis.expire(redisKey,0);
            jedis.close();
        }
    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Connection connection = PhoenixUtil.getConnection();
        JSONObject baseTrademark = getDimData(connection, "DIM_BASE_TRADEMARK", "12");
        System.out.println(baseTrademark);
    }

}
