package com.flink.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

@Mapper
public interface UvMapper {


    @Select("select ch,sum(uv_ct) uv from gmall.dws_traffic_vc_ch_ar_is_new_page_view_window where toYYYYMMDD(stt)=#{date} group by ch order by uv desc")
    public List<Map<String, Object>> getUvByCh(int date);

}
