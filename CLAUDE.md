# CLAUDE.md

本文件为 Claude Code（claude.ai/code）在此仓库中工作时提供指导。

## 项目概览

一个基于 Maven 的 Java 项目，包含 Apache Flink 1.13.1 流/批处理教程示例。代码演示了 Flink 的核心 API：流处理、状态管理、窗口、水位线、处理函数以及 Sink 连接器（JDBC、Kafka）。学习资料来源于 [ddkk.com](https://ddkk.com/zhuanlan/bigdata/flink/1/index.html)。

## 构建与运行

```bash
# 编译打包（fat JAR）
mvn clean package

# 提交到本地 Flink 集群
./bin/flink run -c com.xj.flink.FixedStringJob target/FlinkJava-1.0-SNAPSHOT.jar

# 通过 Maven exec 插件（如果已配置）或 IDE 运行单个类 — 每个 main 类都是独立的 Flink 作业
```

项目没有实际意义的测试（仅有一个 JUnit 3 的桩测试 `AppTest`）。开发方式是在 IDE 中直接运行 `main()` 方法。

## 架构

所有源码位于 `com.xj.flink` 下。每个顶层类都是一个独立的 Flink 作业，拥有自己的 `main()` 方法。所有作业的通用模式：

1. 获取 `StreamExecutionEnvironment.getExecutionEnvironment()`
2. 从元素、socket 或自定义 `SourceFunction` 读取数据
3. 如需事件时间语义，分配水位线
4. 应用转换算子（`map`、`flatMap`、`keyBy`、`window`、`aggregate`、`process` 等）
5. 通过 `print()` 或自定义 `SinkFunction` 输出结果
6. 调用 `env.execute(jobName)` 启动作业

### 关键包

| 包名 | 用途 |
|---|---|
| `com.xj.flink` | 根包 — 入口作业类（词频统计、批处理、流处理） |
| `com.xj.flink.source` | `Event` POJO 数据模型、`ClickSource` 自定义数据源、窗口结果类型 |
| `com.xj.flink.sink` | 自定义 `RichSinkFunction` 实现（文件、带 upsert 的 MySQL JDBC） |
| `com.xj.flink.watermark` | 水位线策略示例 — 单调时间戳、有界乱序 |
| `com.xj.flink.window` | 窗口操作 — 滚动/滑动事件时间窗口、`AggregateFunction` + `ProcessWindowFunction` |
| `com.xj.flink.processfunction` | `KeyedProcessFunction` / `ProcessFunction` 配合定时器、TopN 模式 |
| `com.xj.flink.state` | `ValueState`、`ListState`、`CoProcessFunction` 双流 Join |
| `com.xj.flink.transform` | 转换算子 — `map`、`flatMap`、`filter`、`reduce`、`partition`、富函数 |

### 通用数据模型

`Event`（位于 `com.xj.flink.source`）是大多数示例共用的 POJO：
- `public String user` — 用户标识
- `public String url` — 页面 URL
- `public Long timestamp` — 毫秒级时间戳

`UrlViewCount` 封装窗口聚合结果：`url`、`count`、`windowStart`、`windowEnd`。

### 交互式测试

部分作业从 `env.socketTextStream("localhost", 7777)` 读取数据。运行前需先启动本地监听器：
```bash
nc -lk 7777
```
然后输入匹配源格式的 CSV 数据（例如 `mary,data,1000`）。

### SinkToMySQL1 MySQL 准备

需要本地 MySQL 数据库 `mydb`，包含以下表：
```sql
CREATE TABLE IF NOT EXISTS user_aggr (
    user VARCHAR(255) primary key,
    pv bigint NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### 日志

`resources` 目录下的 `log4j.properties` 配置了 log4j 1.x。根日志级别设为 `error`，输出到控制台。日志文件（`debug.log`、`error.log`）写入 `src/main/resources/`。

### 依赖（pom.xml）

| 依赖 | 版本 | 说明 |
|---|---|---|
| Apache Flink | 1.13.1 | Streaming、客户端、连接器（Kafka、JDBC）、runtime-web、Scala |
| MySQL Connector | 8.0.33 | SinkToMySQL 示例所需的 JDBC 驱动 |
| SLF4J + Log4j | 1.7.30 / 1.2.17 / 2.14.0 | 日志桥接 |
| JUnit | 3.8.1 | test 作用域，极少使用 |

Maven Shade 插件用于构建 fat JAR，主类为 `FixedStringJob`。

## 常见陷阱

- POM 中属性名拼写错误为 `flink-verison`（非 `flink-version`）。添加 Flink 依赖时，保持一致使用 `${flink-verison}`。
- `maven-compiler-plugin` 中 Java source/target 设为 1.8，但 `<maven.compiler.source>` 是 18。只改其中一个而不改另一个会导致编译问题。
- 许多作业使用 `.setParallelism(1)` 以确保输出顺序确定性，仅为演示目的。
- 当在 `flatMap` / `map` 中使用返回泛型类型（如 `Tuple2<String, Integer>`）的 Lambda 表达式时，需要 `TypeHint` — 否则编译器无法推断完整的泛型类型。
