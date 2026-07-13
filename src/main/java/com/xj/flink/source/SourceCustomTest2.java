package com.xj.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import java.util.Random;
public class SourceCustomTest2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Integer> CustomSource = env.addSource(new ParallelCustomSource()).setParallelism(2);
        CustomSource.print();
        env.execute();
    }
    public static class ParallelCustomSource implements ParallelSourceFunction<Integer>{
        private Boolean running  = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running){
                ctx.collect(random.nextInt());
            }
        }
        @Override
        public void cancel() {
            running = false;
        }
    }
}