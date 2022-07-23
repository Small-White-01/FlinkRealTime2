package com.flink.service;

import com.flink.mapper.GmvMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class GmvServiceImpl implements GmvService{


    @Autowired
    private GmvMapper gmvMapper;

    @Override
    public Double getGmv(int date) {
        return gmvMapper.getGMV(date);
    }
}
