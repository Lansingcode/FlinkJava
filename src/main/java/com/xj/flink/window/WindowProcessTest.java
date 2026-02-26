package com.xj.flink.window;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
public class WindowProcessTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        //从元素中读取数据
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                //乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        stream.print("Input_data");
        //使用ProcessWindowFunction计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();
        env.execute();
    }
    //实现自定义的ProcessWindowFunction，输出一条统计信息
    public static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{
        public UvCountByWindow() {
            super();
        }
        @Override
        public void clear(ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context) throws Exception {
            super.clear(context);
        }
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> elements, Collector<String> out) throws Exception {
            //用一个HashSet保存User
            HashSet<String> userSet = new HashSet<>();
            //从elements中遍历数据，放到set中去重
            for(Event event:elements){
                userSet.add(event.user);
            }
            Integer uv = userSet.size();
            //结合窗口信息
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            //输出
            out.collect("窗口 "+ new Timestamp(start) + " ~ " + new Timestamp(end)
                    + " UV值为：" + uv);
        }
    }
}