package com.flink.service;

import com.flink.mapper.UvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@Service
public class UvServiceImpl implements UvService{


    @Autowired
    private UvMapper uvMapper;

    @Override
    public Map<String, BigInteger> getUvByCh(int date) {

        List<Map<String, Object>> mapList = uvMapper.getUvByCh(date);


        HashMap<String,BigInteger> hashMap=new HashMap<>();
        for (Map<String, Object> map:mapList){
            hashMap.put((String) map.get("ch"),(BigInteger) map.get("uv"));
        }



        return hashMap;
    }
}
