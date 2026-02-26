package com.xj.flink.stream;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;
public class UnionStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // nc -lk 127.0.0.1 7777
        SingleOutputStreamOperator<Event> stream1 =
                env.socketTextStream("localhost", 7777)
                        .map(data -> {
                            String[] filed = data.split(",");
                            return new Event(filed[0].trim(), filed[1].trim(), Long.valueOf(filed[2].trim()));
                        })
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));
        stream1.print("stream1的数据：");

        // nc –lk 8888
        SingleOutputStreamOperator<Event> stream2 = env.socketTextStream("localhost", 8888)
                .map(data -> {
                    String[] filed = data.split(",");
                    return new Event(filed[0].trim(), filed[1].trim(), Long.valueOf(filed[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream2.print("stream2的数据：");
        //合并两条流
        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线" + ctx.timerService().currentWatermark());
                    }
                }).print();
        env.execute();
    }
}
