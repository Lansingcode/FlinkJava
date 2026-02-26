package com.xj.flink.transform;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class TransformReduceTest {
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
        //1.统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = Stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    //将Event数据类型转换成元组类型
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.user, 1L);
                    }
                }).keyBy(data -> data.f0) //使用用户名来进行分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        //每到一条数据，用户 pv 的统计值加 1
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        //2.选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clicksByUser
                .keyBy(data -> "key") //为每一条数据分配同一个key，将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });
        result.print();
        env.execute();
    }
}
