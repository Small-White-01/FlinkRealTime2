package com.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderCt {
    private String stt;
    private String edt;
    private String skuId;
    private long ct=0;
    private Long ts;
    private List<String> list=new ArrayList<>();

    public void add(String s){
        list.add(s);
    }

    private static Set<String> set=new HashSet<>();

    public void sAdd(String s){
        set.add(s);
    }

    public int getSetSize(){
        return set.size();
    }
}
