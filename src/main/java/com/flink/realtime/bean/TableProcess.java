package com.flink.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableProcess {

    private String sourceTable;
    private String sinkTable;
    private String sinkPk;
    private String sinkColumns;
    private String sinkExtend;
}
