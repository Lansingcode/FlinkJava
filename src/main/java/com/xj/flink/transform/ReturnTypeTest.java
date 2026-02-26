package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class ReturnTypeTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //从元素中读取数据
        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L)
        );
        // 想要转换成二元组类型 需要进行以下处理
        // 1.使用显式的returns();
        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream.map(data -> Tuple2.of(data.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        result.print();
        // 2.使用类来替代Lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Long>> Result2 = stream.map(new MyTuple2());
        //3.使用匿名类来替代Lambda表达式
        SingleOutputStreamOperator<Tuple2<String, Long>> result3 = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1L);
            }
        });
        result.print("Result:");
        Result2.print("Result2:");
        result3.print("result3:");
        env.execute();
    }
    // 自定义MapFunction类
    public static class MyTuple2 implements MapFunction<Event,Tuple2<String,Long>>{
        @Override
        public Tuple2<String, Long> map(Event value) throws Exception {
            return Tuple2.of(value.user,1L);
        }
    }
}

