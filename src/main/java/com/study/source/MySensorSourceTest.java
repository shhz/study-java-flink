package com.study.source;

import com.study.beans.CollectionSourceBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author shsq
 * @description: MySensorSourceTest
 * @date 2021/3/5 15:08
 */
public class MySensorSourceTest {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从自定义数据源中获取数据
        DataStream<CollectionSourceBean> dataStream = env.addSource(new MySensorSource());

        // 打印输出
        dataStream.print();

        // 执行
        env.execute();
    }

    // 自定义数据源
    public static class MySensorSource implements SourceFunction<CollectionSourceBean> {
        // 定义标志位，控制读取是否需要结束
        private boolean running = true;

        @Override
        public void run(SourceContext<CollectionSourceBean> sourceContext) throws Exception {
            System.out.println("自定义数据源开始工作。");
            // 设定一个随机数发生器
            Random random = new Random();

            // 设置 10 个传感器的初始温度
            HashMap<String, Double> dataMap = new HashMap<>();
            for (int i = 1; i <= 10; i++) {
                // 生成高斯随机数（呈正态分布）
                dataMap.put("source_" + i, 60 + random.nextGaussian() * 17);
            }

            while (running) {
                for (String key : dataMap.keySet()) {
                    Double newTemp = dataMap.get(key) + random.nextGaussian();
                    dataMap.put(key, newTemp);
                    sourceContext.collect(new CollectionSourceBean(key, System.currentTimeMillis(), dataMap.get(key)));
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
            System.out.println("自定义数据源关闭工作。");
        }
    }
}
