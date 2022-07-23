package com.flink.mapper;


import com.flink.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface OrderAmountMapper {


    @Select("select province_name name,sum(order_count) sizeValue,sum(order_amount) value" +
            " from dws_trade_province_order_window where toYYYYMMDD(stt)=#{date} group by province_name")
    public List<TradeProvinceOrderAmount> getOrderByProvince(int date);
}
