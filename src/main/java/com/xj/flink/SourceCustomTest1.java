package com.xj.flink;

import com.xj.flink.source.Event;
import com.xj.flink.source.ParallelClickSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author benjamin_5
 * @Description 读取自定义并行数据源
 * @date 2024/9/24
 */

public class SourceCustomTest1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> CustomSource = env.addSource(new ParallelClickSource()).setParallelism(2);
        CustomSource.print();
        env.execute();
    }

}
