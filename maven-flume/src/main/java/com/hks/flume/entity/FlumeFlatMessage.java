/*
 * 文件名：FlumeFlatMessage.java
 * 版权：Copyright by www.yiche.com
 * 描述：
 * 修改人：hekuangsheng
 * 修改时间：2019/3/18
 * 跟踪单号：
 * 修改单号：
 * 修改内容：
 */
package com.hks.flume.entity;

import com.alibaba.otter.canal.protocol.FlatMessage;

/**
 * MQ的JSON反序列化后的对象
 *
 * @Author: hekuangsheng
 * @Date: 2019/1/15
 */
public class FlumeFlatMessage extends FlatMessage {
    private String dbType;

    public String getDbType() {
        return dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }
}
