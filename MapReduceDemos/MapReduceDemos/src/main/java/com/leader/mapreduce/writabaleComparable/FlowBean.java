package com.leader.mapreduce.writabaleComparable;

/*
 * 1.定义类实现writable接口
 * 2.重写序列化和反序列化方法
 * 3.重写空参构造
 * 4.toString方法
 */

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.webapp.hamlet2.HamletSpec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();

    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowBean o) {
        // 总流量的倒叙
        if (this.sumFlow > o.sumFlow){
            return -1;
        }else if (this.sumFlow < o.sumFlow){
            return 1;
        }else {
            if (this.upFlow > o.upFlow){
                return 1;
            } else if (this.upFlow < o.upFlow) {
                return -1;
            }else {
                return 0;
            }
        }
    }
}
