package com.xj.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;
import java.util.List;
public class StreamMinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List data = new ArrayList<Tuple3<String, String, Integer>>();
        data.add(new Tuple3<>("男", "老王", 80));
        data.add(new Tuple3<>("男", "小王", 25));
        data.add(new Tuple3<>("男", "老李", 85));
        data.add(new Tuple3<>("男", "小李", 20));
        DataStreamSource peoples = env.fromCollection(data);
        //求年龄最小的人
        SingleOutputStreamOperator minResult = peoples.keyBy(0).minBy(2);
        minResult.print();
        env.execute("StreamMinTest");
    }
}