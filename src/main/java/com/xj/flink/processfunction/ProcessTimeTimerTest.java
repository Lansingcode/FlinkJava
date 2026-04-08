package com.xj.flink.processfunction;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
public class ProcessTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        Long CurrTimeStamp = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey() +"数据到达，到达时间： " + new Timestamp(CurrTimeStamp));
                        //注册一个10s后定时器
                        ctx.timerService().registerProcessingTimeTimer(CurrTimeStamp + 10 * 1000L);
                    }
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
