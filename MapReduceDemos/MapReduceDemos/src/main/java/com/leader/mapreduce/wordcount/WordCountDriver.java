package com.leader.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3.关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4.设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass((IntWritable.class));

        // 5.设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("D:\\role.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D:\\output"));

        // 7.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}

//切片源码解析：
//默认情况下，切片大小=blocksize
//每次切片时，都要判断且玩剩下的部分是否大于块的1.1倍，不大于1.1倍就划分一块切片

//排序是MapReduce框架中的重要手段
//MapTask和ReduceTask均会对数据按照key进行排序。该操作是Hadoop的默认行为。任何应用程序中的数据均会被排序，而不管逻辑是否需要。（提高效率）
//默认排序是按照字典顺序排序，且实现该排序的方法是快速排序。

//MapTask并行度由切片个数决定，切片个数由输入文件和切片规则（切片最大最小值）决定

//ReduceTask=0,表示没有Reduce阶段，输出文件个数和Map个数一致
//ReduceTask默认值就是1，所以输出文件个数为一个
//如果数据分布不均匀，就有可能在Reduce阶段产生数据倾斜
//ReduceTask数量并不是任意设置，还要考虑业务逻辑需求，有些情况下，需要计算全局汇总结果，就只能由1个ReduceTask
//具体多少个ReduceTask，需要根据集群性能而定
//如果分区数不是1，但是ReduceTask为1，是否执行分区过程。答案：不执行分区过程。因为在MapTask的源码中，执行分区的前提是先判断ReduceNum个数是否大于1，不大于1肯定不执行。

//MapTask和ReduceTask源码

// ReduceJoin缺点：合并操作实在Reduce阶段完成，Reduce端的处理压力太大，Map节点的运算负载则很低，资源利用率不高，且在Reduce阶段极易产生数据倾斜
// 解决方案： Map端实现数据合并

// Reduce端处理过多的表，非常容易产生数据倾斜。故可在Map端缓存多张表，提前处理业务逻辑，增加Map端业务，减少Reduce端数据的压力，尽可能减少数据倾斜
// 1.在Mapper的setup阶段，将文件读取到缓存集合中 2.在Driver驱动类中加载缓存
