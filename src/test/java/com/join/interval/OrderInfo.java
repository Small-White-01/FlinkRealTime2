package com.join.interval;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class OrderInfo {
    private String id;
    private String name;
    private Long ts;
}
