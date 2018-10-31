package com.hks.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 继承Mapper类需要定义四个输出、输出类型泛型：
 * 四个泛型类型分别代表：
 * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
 * ValueIn      Mapper的输入数据的Value，这里是每行文字
 * KeyOut       Mapper的输出数据的Key，这里是每行文字中的单词"hello"
 * ValueOut     Mapper的输出数据的Value，这里是每行文字中的出现的次数
 *
 * Writable接口是一个实现了序列化协议的序列化对象。
 * 在Hadoop中定义一个结构化对象都要实现Writable接口，使得该结构化对象可以序列化为字节流，字节流也可以反序列化为结构化对象。
 * LongWritable类型:Hadoop.io对Long类型的封装类型
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * 重写Map方法
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
            throws IOException, InterruptedException {
        // 获得每行文档内容，并且进行折分
        String[] words = value.toString().split(" ");

        // 遍历折份的内容
        for (String word : words) {
            // 每出现一次则在原来的基础上：+1
            context.write(new Text(word), new LongWritable(1));
        }
    }
}
