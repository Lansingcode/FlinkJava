package com.xj.flink.state;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.time.Duration;
public class PeriodicPV {
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
        stream.print("数据输入： ");
        //统计每个用户的PV
        stream.keyBy(d -> d.user)
                .process(new PeriodicPVResult())
                .print();
        env.execute();
    }
    //实现自定义的KeyedProcessFunction
    public static class PeriodicPVResult extends KeyedProcessFunction<String,Event,String>{
        //定义状态，保存当前PV统计值,以及有没有定时器
        ValueState<Long> countState;
        ValueState<Long> timerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("Count",Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("Timer",Long.class));
        }
        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，更新对应的count值
            Long count = countState.value();
            countState.update(count == null ? 1 : count + 1);
            //如果没有注册过定时器，注册定时器
            if(timerState.value() == null ){
                ctx.timerService().registerEventTimeTimer(value.timestamp + 10 * 1000L);
                timerState.update(value.timestamp + 10 * 1000L);
            }
        }
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出统计结果
            out.collect(ctx.getCurrentKey() + "PV: " + countState.value());
            //清空状态
            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + 10 * 1000L);
            timerState.update(timestamp + 10 * 1000L);
        }
    }
}
