package com.flink.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {

    public static List<String> split(String word) throws IOException {

        IKSegmenter ikSegmenter = new IKSegmenter(new StringReader(word), false);
        List<String> list=new ArrayList<>();
        Lexeme next = null;
        while ((next=ikSegmenter.next())!=null){
            list.add(next.getLexemeText());
        }
        return list;
    }

    public static void main(String[] args) throws IOException {
        List<String> strings = split("图书");
        System.out.println(strings);

    }
}
