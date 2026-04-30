package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;

public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> Stream = env.fromElements(
                new Event("Mary", "./home/123", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        //进行转计算,提取usr字段
        //1.使用自定义类实现MapFunction接口
        SingleOutputStreamOperator<String> result1 = Stream.map(new MyMapper());
        //2.使用匿名类实现MapFunction接口
        SingleOutputStreamOperator<String> result2 = Stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.user;
            }
        });
        //3.传入Lambda表达式  对于类里面只有一个方法的接口 可以使用Lambda表达式
//        SingleOutputStreamOperator<String> result3 = Stream.map(date -> date.user);
        SingleOutputStreamOperator<String> result3 = Stream.flatMap(
                (Event value, Collector<String> out) -> {
                    for (String s : value.url.split("/")) {
                        if (!s.isEmpty()) {
                            out.collect(s);
                        }
                    }
                }
        ).returns(new TypeHint<String>() {});


        SingleOutputStreamOperator<String> result4 = Stream.map(date -> "用户"+date.user+"在"+date.timestamp+"访问了"+date.url);

        result1.print();
        result2.print();
        result3.print();
        result4.print();
        env.execute();
    }
    ///自定义MapFunction
    public static class MyMapper implements MapFunction<Event,String>{
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}