package com.xj.flink.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ParallelClickSource implements ParallelSourceFunction<Event> {
    private Boolean running  = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Marry","Alice","Bob","Jek"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100","./prod?id=199"};
        //循环生产数据
        while (running){
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}
