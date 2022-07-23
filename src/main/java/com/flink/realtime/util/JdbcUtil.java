package com.flink.realtime.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 通用的JDBC，应付各个connection
 */
public class JdbcUtil {

    public static <T> List<T> query(Connection connection,String sql,Class<T> tClass,boolean upperCamel) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {

        ArrayList<T> list=new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);


        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();

        while (resultSet.next()){
            T t = tClass.newInstance();
            for (int i = 0; i < metaData.getColumnCount(); i++) {

                String columnName = metaData.getColumnName(i + 1);
                String value = resultSet.getString(columnName);
                if(upperCamel){
                    columnName= CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                BeanUtils.setProperty(t,columnName,value);
            }
            list.add(t);

        }

        preparedStatement.close();;
        resultSet.close();
        return list;

    }

    public static void main(String[] args) throws SQLException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Connection connection = PhoenixUtil.getConnection();
        List<JSONObject> list = query(connection, "select * from GMALL.DIM_BASE_TRADEMARK where id='12'", JSONObject.class, false);
        System.out.println(list);
    }
}
