package com.flink.realtime;

import redis.clients.jedis.Response;

import java.util.BitSet;

public class LocalBloomFilter {



    private int size;
    private int elements;
    private int hashCount;
    BitSet bitSet;

    public LocalBloomFilter(int elements) {
        this.elements = elements;
        this.hashCount=8;
        this.size= (int) (hashCount*Math.abs(elements)/Math.log(2.0));
        bitSet=new BitSet(size);
    }

    public void put(String word){
        for (int i = 0; i <word.length() ; i++) {
            String substring = word.substring(i);
            int hash = hash(substring);
            int offset=hash%size;
            bitSet.set(offset,true);
        }


    }
    public boolean exist(String word){
        boolean flag=true;
        for (int i = 0; i <word.length() ; i++) {
            String substring = word.substring(i);
            int hash =hash(substring);
            int offset=hash%size;
            boolean b = bitSet.get(offset);
            flag=flag&b;
        }
        return flag;
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

    public static void main(String[] args) {
        LocalBloomFilter localBloomFilter = new LocalBloomFilter(100000);
        localBloomFilter.put("aaaa");
        System.out.println(localBloomFilter.exist("aaaa"));
        System.out.println(localBloomFilter.exist("asac"));

//
//        long l = System.currentTimeMillis();
//        for (long i = 0; i < 400000000L; i++) {
//            localBloomFilter.put("aaaa");
//            localBloomFilter.exist("aaaa");
//        }
//        System.out.println(System.currentTimeMillis()-l);//143377ms
        MyBloomFilter myBloomFilter = new MyBloomFilter(100000);
        long l2 = System.currentTimeMillis();
        for (long i = 0; i < 400000000L; i++) {
            myBloomFilter.put("aaaa");
            myBloomFilter.exist("aaaa");
        }
        myBloomFilter.del("aaaa");
        System.out.println(System.currentTimeMillis()-l2);
    }
}
