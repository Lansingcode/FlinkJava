
在SinkToMySQL1.java中运行方式如下：
在命令行中运行`nc -lk 7777`监听7777端口
在MySQL数据库中增加表

```angular2html
CREATE TABLE IF NOT EXISTS user_aggr (
    user VARCHAR(255) primary key,
    pv bigint NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```
其中user为主键  
在端口界面输入数据

```angular2html
mary,data,1000
bob,data,2000
```

CLICKS表结构
```angular2html
drop table if exists CLICKS;
CREATE TABLE CLICKS(
USER VARCHAR(100) NOT NULL
,URL VARCHAR(100) NOT NULL
,timestamp BIGINT DEFAULT NULL
);
```