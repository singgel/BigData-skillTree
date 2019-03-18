/*
 * 文件名：RedisTest.java
 * 版权：Copyright by www.yiche.com
 * 描述：
 * 修改人：hekuangsheng
 * 修改时间：2019/3/15
 * 跟踪单号：
 * 修改单号：
 * 修改内容：
 */
package com.hks.flink;

import redis.clients.jedis.Jedis;

public class RedisTest {
    public static void main(String args[]){
        Jedis jedis=new Jedis("127.0.0.1");
        System.out.println("Server is running: " + jedis.ping());
        System.out.println("result:"+jedis.hgetAll("flink"));
    }
}
