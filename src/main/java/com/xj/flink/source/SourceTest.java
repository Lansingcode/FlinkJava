package com.xj.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度1
        env.setParallelism(1);
        //从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);
        numStream.print("1");

        //从文件中读取数据
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        events.add(new Event("Alice","./prod?id=100",3000L));
        DataStreamSource<Event> Stream = env.fromCollection(events);
        Stream.print("1");
        env.execute();
    }
}