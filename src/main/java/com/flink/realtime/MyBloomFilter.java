package com.flink.realtime;

import com.flink.realtime.util.JedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class MyBloomFilter {



    private int size;
    private int elements;
    private int hashCount;

    public MyBloomFilter(int elements) {
        this.elements = elements;
        this.hashCount=8;
        this.size= (int) (hashCount*Math.abs(elements)/Math.log(2.0));

    }

    static int hash(String s){
        int hash=0;
        if(s!=null&&s.length()>0) {
            for (int i = s.length() - 1; i >= 0; i--) {
                hash += 31 * hash + s.charAt(i);
            }
        }
        return hash;
    }

    public Response<String> put(String word){
        Jedis jedis = JedisUtil.getResource();
        Pipeline pipelined = jedis.pipelined();
        for (int i = 0; i <word.length() ; i++) {
            String substring = word.substring(i);
            long hash = hash(substring);
            long offset=hash%size;


            pipelined.setbit(word,offset,true);
        }


        Response<String> stringResponse = pipelined.multi();

        pipelined.close();
        jedis.close();
        return stringResponse;
//        word.substring(0,)
//        pipelined.setbit()
    }

    //自定义布隆
    public boolean exist(String word){
        Jedis jedis = JedisUtil.getResource();
        Pipeline pipelined = jedis.pipelined();
        boolean flag=true;
        List<Response<Boolean>> booleanList=new ArrayList<>();
        for (int i = 0; i <word.length() ; i++) {
            String substring = word.substring(i);
            long hash =hash(substring);
            long offset=hash%size;

            Response<Boolean> getbit = pipelined.getbit(word, offset);
            booleanList.add(getbit);
        }


        pipelined.close();
        jedis.close();
        for (Response<Boolean> response:booleanList){
            flag=response.get()&flag;
        }

        return flag;
    }

    public long del(String ...k){
        Jedis resource = JedisUtil.getResource();
        Long del = resource.del(k);
        return del;
    }

    public static void main(String[] args) {
        String s="asjjca";
//        System.out.println(hash(s));
//        System.out.println(s.hashCode());
        MyBloomFilter myBloomFilter = new MyBloomFilter(10000);
        Response<String> put = myBloomFilter.put(s);
//        String objects = put.get();
//        System.out.println(objects);
        boolean exist = myBloomFilter.exist(s);
        System.out.println(exist);
        System.out.println(myBloomFilter.exist("asjjca"));
        System.out.println(myBloomFilter.del(s));
        System.out.println(myBloomFilter.exist(s));





    }

}
