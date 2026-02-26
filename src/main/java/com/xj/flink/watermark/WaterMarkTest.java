package com.xj.flink.watermark;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;
public class WaterMarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        //从元素中读取数据
        SingleOutputStreamOperator<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 1500L),
                new Event("Alice", "./prod?id=100", 1800L),
                new Event("Bob", "./prod?id=1", 2000L),
                new Event("Alice", "./prod?id=200", 3000L),
                new Event("Bob", "./home", 2500L),
                new Event("Bob", "./prod?id=120", 3600L),
                new Event("Bob", "./prod?id=130", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));
        env.execute();
    }
}