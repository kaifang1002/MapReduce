package com.leader.mapreduce.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();

        boolean result = parseLog(line, context);
        if (!result) {
            return;
        }
        context.write(value, NullWritable.get());
    }

    private boolean parseLog(String line, Mapper<LongWritable, Text, Text, NullWritable>.Context context) {
        String[] fileds = line.split(" ");
        if (fileds.length > 11) {
            return true;
        } else {
            return false;
        }
    }
}
