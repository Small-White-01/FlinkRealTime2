package com.flink.service;

import com.flink.bean.TradeProvinceOrderAmount;

import java.util.List;

public interface OrderAmountService {

    public List<TradeProvinceOrderAmount> getOrderByProvince(int date);
}
