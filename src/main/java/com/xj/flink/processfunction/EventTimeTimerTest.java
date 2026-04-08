package com.xj.flink.processfunction;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;
public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new CustomSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        // 时间定时器
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, Object>() {
                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Object>.OnTimerContext ctx, Collector<Object> out) throws Exception {
                        out.collect(ctx.getCurrentKey() +
                                "定时器触发，触发时间：" + new Timestamp(timestamp) +
                                " WaterMarks: " + ctx.timerService().currentWatermark());
                    }
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, Object>.Context ctx, Collector<Object> out) throws Exception {
                        Long currTs =  ctx.timestamp();
                        out.collect(ctx.getCurrentKey()
                                + "数据到达，时间戳： " + new Timestamp(currTs) +
                                " 当前 WaterMarks: " + ctx.timerService().currentWatermark());
                        //注册一个10s的定时器
                        ctx.timerService().registerEventTimeTimer(currTs + 10 * 1000L);
                    }
                }).print();
        env.execute();
    }
    //自定义测试数据源
    public static class CustomSource implements SourceFunction<Event>{
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            //直接发出测试数据
            ctx.collect(new Event("Mary","www.baidu.com",1000L));
            // 为了测试明显，中间停顿5s
            Thread.sleep(5000L);
            // 发出 10 秒后的数据
            ctx.collect(new Event("Bob","www.bing.com",11000L));
            Thread.sleep(5000L);
            // 发出 10 秒+1ms 后的数据
            ctx.collect(new Event("Alice","www.apple.com",11001L));
            Thread.sleep(5000L);
        }
        @Override
        public void cancel() {
        }
    }
}
