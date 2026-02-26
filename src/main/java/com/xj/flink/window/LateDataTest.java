package com.xj.flink.window;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import com.xj.flink.source.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import java.time.Duration;
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        // 将数据源改为 socket 文本流，并转换成 Event类型
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                });
        //// 方式一、针对乱序流插入水位线，延迟时间设置为2s
        SingleOutputStreamOperator<Event> streams = stream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timestamp;
                    }
                }));
        streams.print("输入数据:");
        //定义一个输出标签
        OutputTag<Event> late = new OutputTag<Event>("late"){};
        SingleOutputStreamOperator<UrlViewCount> result = streams.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //方式二：允许窗口处理迟到数据，设置 1 分钟的等待时间
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());
        result.print("输出数据：");
        //方式三：将最后的迟到数据输出到侧输出流
        result.getSideOutput(late).print("侧输出流：");
        env.execute();
    }
}
