package com.xj.flink.stream;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
public class SplitStreamTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        //定义输出标签
        OutputTag<Tuple3<String, String, Long>> MarryTag = new OutputTag<Tuple3<String,String,Long>>("Mary"){};
        OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String,String,Long>>("Bob"){};
        SingleOutputStreamOperator<Event> processStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.user.equals("Marry")) {
                    ctx.output(MarryTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else if (value.user.equals("Bob")) {
                    ctx.output(BobTag, Tuple3.of(value.user, value.url, value.timestamp));
                } else
                    out.collect(value);
            }
        });
        processStream.print("主流：");
        processStream.getSideOutput(MarryTag).print("Marry相关数据： ");
        processStream.getSideOutput(BobTag).print("Bob相关数据： ");
        env.execute();
    }
}
