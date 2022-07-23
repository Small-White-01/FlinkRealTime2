package com.flink.service;

import com.flink.bean.TradeProvinceOrderAmount;
import com.flink.mapper.OrderAmountMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class OrderAmountServiceImpl implements OrderAmountService{


    @Autowired
    private OrderAmountMapper orderAmountMapper;

    @Override
    public List<TradeProvinceOrderAmount> getOrderByProvince(int date) {

        List<TradeProvinceOrderAmount> orderByProvince = orderAmountMapper.getOrderByProvince(date);
        List<TradeProvinceOrderAmount> orderAmountList = orderByProvince.stream().map(new Function<TradeProvinceOrderAmount, TradeProvinceOrderAmount>() {
            @Override
            public TradeProvinceOrderAmount apply(TradeProvinceOrderAmount tradeProvinceOrderAmount) {
                tradeProvinceOrderAmount.getTooltipValues().add(tradeProvinceOrderAmount.getSizeValue());
                return tradeProvinceOrderAmount;
            }
        }).collect(Collectors.toList());
        return orderAmountList;
    }
}
