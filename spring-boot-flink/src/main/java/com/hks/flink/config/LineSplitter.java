/*
 * 文件名：LineSplitter.java
 * 版权：Copyright by www.yiche.com
 * 描述：
 * 修改人：hekuangsheng
 * 修改时间：2019/3/15
 * 跟踪单号：
 * 修改单号：
 * 修改内容：
 */
package com.hks.flink.config;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        String[] tokens = value.toLowerCase().split("\\W+");
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
