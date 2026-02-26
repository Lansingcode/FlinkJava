package com.xj.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * @author benjamin_5
 * @Description 读取本地文件，采用批量计算方式统计词频
 * @date 2024/9/24
 */
public class FileStringBatchJob {

    // 启动本地flink ./bin/start-cluster.sh
    // 切换到flink安装目录：cd /usr/local/flink-1.20.0
    // 提交jar包到集群运行：./bin/flink run -sae -c com.xj.flink.FixedStringJob /Users/xj/Documents/IDEAProjects/FlinkJava/target/FlinkJava-1.0-SNAPSHOT.jar
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // get input data
        String filePath = "file:///Users/xj/Documents/IDEAProjects/FlinkJava/src/main/resources/words.txt";
        final String output = "file:///Users/xj/Documents/IDEAProjects/FlinkJava/src/main/resources/output.txt";
        DataSet<String> text = env.getJavaEnv().readTextFile(filePath);

        DataSet<Tuple2<String, Integer>> counts = text
                .flatMap(new MyFlatMapFunction())// split up the lines in pairs (2-tuples) containing: (word,1)
                .groupBy(0)
                .aggregate(Aggregations.SUM, 1);// group by the tuple field "0" and sum up tuple field "1"

        //            counts.addSink(new FileStringJob.MySink());
        counts.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        System.out.println("执行完成");
        // 执行
        env.execute("WordCount Batch Process");
    }

    public static class MySink extends RichSinkFunction<Tuple2<String, Integer>> {
        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            String world = value.getField(0);
            Integer count = value.getField(1);
            // 输出
            System.out.println("单词："+world + "，次数："+count);
        }
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }
    }
}
