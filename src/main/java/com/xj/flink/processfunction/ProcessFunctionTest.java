package com.xj.flink.processfunction;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;
public class ProcessFunctionTest {
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
        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
                if(value.user.equals("Marry")){
                    out.collect(value.user + " clicks " + value.url);
                } else if(value.user.equals("Bob")){
                    out.collect(value.user);
                    out.collect(value.user);
                }
                out.collect(value.toString());
                //当前时间
                System.out.println("TimeStamp: " + ctx.timestamp());
                //当前水位线
                System.out.println("WaterMark: " + ctx.timerService().currentWatermark());
                System.out.println(getRuntimeContext().getIndexOfThisSubtask());
            }
            @Override
            public void onTimer(long timestamp, ProcessFunction<Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }
        }).print();
        env.execute();
    }
}