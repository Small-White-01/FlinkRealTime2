package com.flink.realtime.func;

import com.flink.realtime.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword){



        try {
                List<String> split = KeyWordUtil.split(keyword);
         //   System.out.println(split);
                for (String word: split) {
                    collect(Row.of(word));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }


    }

}
