package com.xj.flink.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SinkToMySQL1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //执行前先监听端口：nc -lk 7777
        DataStreamSource<String> lineDataStream = env.socketTextStream("127.0.0.1", 7777);

        DataStream<Tuple2<String, Long>> userAndOneTuple = lineDataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Tuple2<String, Long>(fields[0],1L);
            }
        })
                .keyBy(data -> data.f0)
                .sum(1);

        userAndOneTuple.addSink(new MySQLSink());
        //6.打印
//        userAndOneTuple.print();

        env.execute();
    }

    /**
     * 自定义 MySQL Sink
     */
    public static class MySQLSink extends RichSinkFunction<Tuple2<String, Long>> {

        private Connection connection;
        private PreparedStatement insertStmt;
        private PreparedStatement updateStmt;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // MySQL 连接配置
            String url = "jdbc:mysql://localhost:3306/mydb";
            String username = "root";
            String password = "rootroot";

            // 加载 MySQL 驱动
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立连接
            connection = DriverManager.getConnection(url, username, password);


            // 准备 SQL 语句 (使用 ON DUPLICATE KEY UPDATE 实现 upsert)
            String sql = "INSERT INTO user_aggr (user, pv) VALUES (?, ?) ON DUPLICATE KEY UPDATE pv=?";
            insertStmt = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
            // 设置参数
            insertStmt.setString(1, value.f0);      // word
            insertStmt.setLong(2, value.f1);         // count
            insertStmt.setLong(3, value.f1);         // count for update

            // 执行插入/更新
            insertStmt.executeUpdate();
        }

        @Override
        public void close() throws Exception {
            super.close();

            // 关闭资源
            if (insertStmt != null) {
                insertStmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
}