package com.flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TradeProvinceOrderAmount {




    private String name;
    private BigInteger value;
    private BigInteger sizeValue;
    private List<BigInteger> tooltipValues=new ArrayList<>();


}
