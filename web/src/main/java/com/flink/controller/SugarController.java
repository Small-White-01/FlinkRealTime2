package com.flink.controller;


import com.alibaba.fastjson.JSONObject;
import com.flink.bean.TradeProvinceOrderAmount;
import com.flink.service.GmvService;
import com.flink.service.OrderAmountService;
import com.flink.service.UvService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
@RequestMapping("/")
public class SugarController {


    @Autowired
    public GmvService gmvService;
    @Autowired
    private UvService uvService;
    @Autowired
    private OrderAmountService orderAmountService;

    public static SimpleDateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");

    @RequestMapping("/gmv/value")
    public String getGmv(@RequestParam(value = "dt",defaultValue = "0") int date){
        if(date==0){
            date=getToday();
        }
        Double gmvPrice = gmvService.getGmv(date);
        BigDecimal bigDecimal = BigDecimal.valueOf(gmvPrice).setScale(2, RoundingMode.HALF_UP);
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("status",0);
        jsonObject.put("msg","");
        jsonObject.put("data",bigDecimal.doubleValue());
        return jsonObject.toJSONString();
    }

    @RequestMapping("/ch-uv/value")
    public String getUvByCh(@RequestParam(value = "dt",defaultValue = "0") int date){
        if(date==0){
            date=getToday();
        }
        Map<String, BigInteger> map = uvService.getUvByCh(date);
        Set<String> set = map.keySet();
        Collection<BigInteger> values = map.values();
        return "{ " +
                "  \"status\": 0, " +
                "  \"msg\": \"\", " +
                "  \"data\": { " +
                "    \"categories\": [ \"" +
                StringUtils.join(set.toArray(),"\", \"")+
                "\"], " +
                "    \"series\": [ " +
                "      { " +
                "        \"name\": \"日活\", " +
                "        \"data\": [ " +
                StringUtils.join(values,",")+
                "        ] " +
                "      } " +
                "    ] " +
                "  } " +
                "}";
    }


    @RequestMapping("/province-order/value")
    public String getOrderByProvince(@RequestParam(value = "dt",defaultValue = "0") int date) {
        if (date == 0) {
            date = getToday();
        }

        List<TradeProvinceOrderAmount> orderByProvince = orderAmountService.getOrderByProvince(date);
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("status",0);
        jsonObject.put("msg","");
        JSONObject data = new JSONObject();
        data.put("mapData",orderByProvince);
        data.put("valueName","订单总额");
        data.put("tooltipNames", Collections.singletonList("订单总数"));
        data.put("tooltipUnits", Collections.singletonList("件"));
//        data.put("sizeValueName","订单总数");
        jsonObject.put("data",data);
        return jsonObject.toJSONString();

    }
    @RequestMapping("test")
    public String test(){
        return "success";
    }

    private int getToday() {
        long l = System.currentTimeMillis();
        String date = dateFormat.format(new Date(l));
        return Integer.parseInt(date);
    }


}
