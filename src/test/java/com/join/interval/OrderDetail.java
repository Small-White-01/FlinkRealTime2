package com.join.interval;


import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderDetail {
    private String skuId;
    private String orderId;
    private Long ts;


}
