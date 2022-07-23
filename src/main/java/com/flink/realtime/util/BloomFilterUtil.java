package com.flink.realtime.util;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;

public class BloomFilterUtil {
    static {
        renew();
    }
    public static BloomFilter<String> bloomFilter;


    public static void put(String ele){

        bloomFilter.put(ele);
    }

    public static boolean contain(String value){
        return bloomFilter.mightContain(value);
    }

    public static long getElementCount(){
        return bloomFilter.approximateElementCount();
    }

    public static void renew(){
        bloomFilter=BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()),
                1000000,0.01);
    }

    public static void main(String[] args) {
        put("2000");
        System.out.println(contain("4000"));
    }
}
