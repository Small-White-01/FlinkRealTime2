package com.flink.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {


    static ThreadPoolExecutor threadPoolExecutor;

    public static ThreadPoolExecutor getThreadPoolExecutor() {
        if(threadPoolExecutor==null){
            synchronized (ThreadPoolUtil.class){
                if(threadPoolExecutor==null){
                    threadPoolExecutor=new ThreadPoolExecutor(8,
                            20,60, TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return threadPoolExecutor;
    }
}
