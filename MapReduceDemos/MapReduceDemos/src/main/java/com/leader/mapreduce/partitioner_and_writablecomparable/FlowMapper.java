package com.leader.mapreduce.partitioner_and_writablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context) throws IOException, InterruptedException {

        // 获取每一行
        String line = value.toString();

        // 切割
        String[] split = line.split("\t");

        // 抓取想要的数据
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];

        // 封装
        outK.set(phone);
        outV.setUpFlow(Long.parseLong(up));
        outV.setDownFlow(Long.parseLong(down));
        outV.setSumFlow();

        // 写出
        context.write(outK, outV);
    }
}