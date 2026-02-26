package com.xj.flink.sink;

import com.xj.flink.source.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import java.util.concurrent.TimeUnit;
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
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
        StreamingFileSink<String> stringStreamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./OutPut"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .build()
                )
                .build();
        Stream.map(data -> data.toString())
                .addSink(stringStreamingFileSink);
        env.execute();
    }
}