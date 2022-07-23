package com.flink.realtime.util;

import com.flink.realtime.bean.RedisConstants;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {

    static JedisPool jedisPool;
    public static void initPool(){

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);
        jedisPool=new JedisPool(jedisPoolConfig,RedisConstants.host,RedisConstants.port);
    }

    public static Jedis getResource(){
        if(jedisPool==null|| jedisPool.isClosed()){
            initPool();
        }
        return jedisPool.getResource();
    }


}
