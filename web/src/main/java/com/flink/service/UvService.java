package com.flink.service;

import java.math.BigInteger;
import java.util.Map;

public interface UvService {

    Map<String, BigInteger> getUvByCh(int date);

}
