package com.xj.flink.trans;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class SplitStreamByFilter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());
        // 筛选 Mary 的浏览行为放入 MaryStream 流中
        DataStream<Event> MaryStream = stream.filter(new FilterFunction<Event>()
        {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Marry");
            }
        });
        // 筛选Bob的购买行为放入 BobStream 流中
        DataStream<Event> BobStream = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event value) throws Exception {
                return value.user.equals("Bob");
            }
        });
        // 筛选其他人的浏览行为放入 elseStream 流中
        DataStream<Event> elseStream = stream.filter(new FilterFunction<Event>()
        {
            @Override
            public boolean filter(Event value) throws Exception {
                return !value.user.equals("Marry") && !value.user.equals("Bob") ;
            }
        });
        MaryStream.print("Marry记录：");
        BobStream.print("Bob记录： ");
        elseStream.print("其他： ");
        env.execute();
    }
}
