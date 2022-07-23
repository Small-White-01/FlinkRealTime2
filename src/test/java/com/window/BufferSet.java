package com.window;

import java.util.HashSet;
import java.util.Set;

public class BufferSet {
    private static Set<String> set=new HashSet<>();
    public static void sAdd(String s){
        set.add(s);
    }

    public static int getSetSize(){
        return set.size();
    }
    public static boolean exist(String key){
        return set.contains(key);
    }

    public static void clear() {
        set.clear();
    }
}
