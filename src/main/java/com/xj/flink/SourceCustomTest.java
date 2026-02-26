package com.xj.flink;

import com.xj.flink.source.ClickSource;
import com.xj.flink.source.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author benjamin_5
 * @Description 读取自定义数据源，数据源并行度为1,输出并行度为4
 * @date 2024/9/24
 */

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
//        env.setParallelism(1);
        env.setParallelism(4);
        DataStreamSource<Event> CustomSource = env.addSource(new ClickSource());
        CustomSource.print();
        env.execute();
    }
}