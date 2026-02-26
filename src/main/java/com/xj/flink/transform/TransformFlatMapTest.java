package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> Stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        //1.实现FlatMapFunction
        // Stream.flatMap(new MyFlatMap()).print();
        //2.传入一个匿名类实现FlatMapFunction接口
        SingleOutputStreamOperator<String> result1 = (SingleOutputStreamOperator<String>) Stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event value, Collector<String> out) throws Exception {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        });
        //3.直接传入Lambda表达式
        SingleOutputStreamOperator<String> result2 = Stream.flatMap((Event value, Collector<String> out) -> {
            if (value.user.equals("Mary"))
                out.collect(value.url);
            else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
                out.collect(value.timestamp.toString());
            }
        }).returns(new TypeHint<String>() {});
        result2.print();
        env.execute();
    }
    //实现一个自定义的FlatMapFunction
    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }
}