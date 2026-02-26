package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> Stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Bob", "./home", 3300L),
                new Event("Bob", "./prod?id=120", 3600L),
                new Event("Bob", "./prod?id=130", 4000L)
        );
        //按键分组之后进行聚合,提取当前用户最后一次访问数据
        Stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print("Max");
        //指定字段的名称
//        Stream.keyBy(data -> data.user).maxBy("timestamp").print("MapBy");
        env.execute();
    }
}