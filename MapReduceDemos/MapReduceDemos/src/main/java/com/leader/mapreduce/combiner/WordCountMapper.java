package com.leader.mapreduce.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/* mapper的输出是reduce的输入
* KEYIN, map阶段输入的key的类型：LongWritable
* VALUEIN, map阶段输入的value的类型：Text
* KEYOUT, map阶段输出的key的类型：Text
* VALUEOUT, map阶段输出的value的类型：IntWritable
*/
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    private Text outk = new Text();
    private IntWritable outv = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 切割
        String[] words = line.split(" ");

        // 循环写出
        for (String word: words){

            // 封装
            outk.set(word);

            // 写出
            context.write(outk, outv);
        }
    }
}
