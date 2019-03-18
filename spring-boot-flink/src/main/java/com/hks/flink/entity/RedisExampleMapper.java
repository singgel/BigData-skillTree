/*
 * 文件名：RedisExampleMapper.java
 * 版权：Copyright by www.yiche.com
 * 描述：
 * 修改人：hekuangsheng
 * 修改时间：2019/3/15
 * 跟踪单号：
 * 修改单号：
 * 修改内容：
 */
package com.hks.flink.entity;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public final class RedisExampleMapper implements RedisMapper<Tuple2<String,Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "flink");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
        return data.f1.toString();
    }
}
