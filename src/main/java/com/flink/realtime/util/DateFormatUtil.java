package com.flink.realtime.util;

;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateFormatUtil {

    static SimpleDateFormat dateFormat=new SimpleDateFormat("yyyy-MM-dd");
    public static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

//    public static long toTs2(String date) throws ParseException {
//        return dateFormat.parse(date).getTime();
//    }

    public static long dateToTs(String dtr){
        LocalDateTime parse=null;
        dtr+=" 00:00:00";
        parse= LocalDateTime.parse(dtr, dateTimeFormatter);
        return parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
    public static long dateTimeToTs(String dtr){
        LocalDateTime parse=null;
        parse= LocalDateTime.parse(dtr, dateTimeFormatter);
        return parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

//    public static long toTs(String dtr){
//        return toTs(dtr,true);
//    }

    public static String toDate(long ts){
        Date date = new Date(ts);
        Instant instant = date.toInstant();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return dateFormatter.format(localDateTime);
    }
    public static String toDateTime(long ts){
        Date date = new Date(ts);
        Instant instant = date.toInstant();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        return dateTimeFormatter.format(localDateTime);
    }

    public static void main(String[] args) throws ParseException {
//        String str="2020-12-12 12:04:01";
//        long ts = toTs(str, true);
//        System.out.println(ts);
//        System.out.println(toDate(ts));
//        System.out.println(toDateTime(ts));
//        Date parse = dateFormat.parse(str);
//        System.out.println(parse);
//

        String str1="2020-12-12 12:12:12.876Z";
        System.out.println(dateTimeToTs(str1));
//        System.out.println(toTs2(str1));
    }


}