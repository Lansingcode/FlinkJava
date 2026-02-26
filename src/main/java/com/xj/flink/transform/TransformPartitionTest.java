package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> Stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1500L),
                new Event("Alice", "./prod?id=100", 1800L),
                new Event("Bob", "./prod?id=1", 2000L),
                new Event("Alice", "./prod?id=200", 3000L),
                new Event("Bob", "./home", 2500L),
                new Event("Bob", "./prod?id=120", 3600L),
                new Event("Bob", "./prod?id=130", 4000L)
        );
        //1、随机分区
        Stream.shuffle().print().setParallelism(4);
        env.execute();
    }
}