/*
 * 文件名：ETLInterceptor.java
 * 版权：Copyright by www.yiche.com
 * 描述：
 * 修改人：hekuangsheng
 * 修改时间：2019/3/18
 * 跟踪单号：
 * 修改单号：
 * 修改内容：
 */
package com.hks.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.hks.flume.entity.FlumeFlatMessage;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class ETLInterceptor implements Interceptor {

    private static Logger logger = LoggerFactory.getLogger(ETLInterceptor.class);

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        String body = new String(event.getBody(), Charsets.UTF_8);
        FlumeFlatMessage redisFlatMessage = JSON.parseObject(body, new TypeReference<FlumeFlatMessage>() {
        });
        StringBuilder stringBuilder = new StringBuilder();
        for (Map<String, String> map : redisFlatMessage.getData()) {
            stringBuilder.append(JSON.toJSON(map));
        }
        event.setBody(stringBuilder.toString().getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
        for (Event event : events) {
            Event interceptedEvent = intercept(event);
            if (interceptedEvent != null) {
                intercepted.add(interceptedEvent);
            }
        }
        return intercepted;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new ETLInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
