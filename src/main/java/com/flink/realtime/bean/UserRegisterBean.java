package com.flink.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserRegisterBean {


    private String stt;
    private String edt;
    private Long registerCt;
    private Long ts;
}
