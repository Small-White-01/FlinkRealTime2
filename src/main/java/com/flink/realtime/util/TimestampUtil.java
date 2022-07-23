package com.flink.realtime.util;

public class TimestampUtil {
    /**
     * 处理flink table的timestamp_ltz格式，如2020-12-12 12:12:12.876Z
     */
    public static int compare(String ts1,String ts2){
        long dateTimeTs1 = fromTimeStampLtz(ts1);
        long dateTimeTs2 = fromTimeStampLtz(ts2);
        return dateTimeTs1-dateTimeTs2>0?1:dateTimeTs1==dateTimeTs2?0:-1;
    }

    public static long fromTimeStampLtz(String ts1){
        String tsWithoutZone1 = ts1.substring(0, ts1.length() - 1);
        String[] split1 = tsWithoutZone1.split("\\.");
        String milli1 = (split1[split1.length - 1]+"000").substring(0,3);
        long dateTimeTs1 = DateFormatUtil.dateTimeToTs(split1[0]);
        dateTimeTs1+=Integer.parseInt(milli1);
        return dateTimeTs1;
    }


    public static void main(String[] args) {
        System.out.println(compare("2022-04-01 11:10:55.040Z",
                "2022-04-01 11:10:55.04Z"
        ));
        System.out.println(compare("2022-04-01 11:10:55.040Z",
                "2022-04-01 11:10:55.04Z"
        ));
        System.out.println(compare("2022-04-01 11:10:55.040Z",
                "2022-04-01 11:10:55.04Z"
        ));
    }

}
