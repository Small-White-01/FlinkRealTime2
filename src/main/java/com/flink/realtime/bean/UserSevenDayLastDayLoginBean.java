package com.flink.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserSevenDayLastDayLoginBean {

    private String stt;
    private String edt;
    private Long backCt;
    private Long uuCt;
    private Long ts;

}
