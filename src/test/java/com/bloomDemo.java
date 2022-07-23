package com;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.nio.charset.Charset;

public class bloomDemo {
    public static void main(String[] args) {

//        BloomFilter<CharSequence> charSequenceBloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1000000, 0.01);
//        charSequenceBloomFilter.put("1"+"1");
//        charSequenceBloomFilter.put("aa");
//        charSequenceBloomFilter.put("acc");
//        System.out.println(charSequenceBloomFilter.mightContain("1"+"1"));
//        System.out.println(charSequenceBloomFilter.approximateElementCount());
//        BloomFilter<CharSequence> copy = charSequenceBloomFilter.copy();
//        System.out.println(copy.mightContain("acc"));

        System.out.println(Integer.MAX_VALUE);
        System.out.println(Math.pow(2,31));
        System.out.println("agjjca".hashCode()&(Integer.MAX_VALUE-1));

    }
}
