package com.study.source;

import com.study.beans.CollectionSourceBean;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author shsq
 * @description: CollectionSourceTest
 * @date 2021/3/5 10:51
 */
public class CollectionSourceTest {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合获取数据
        DataStream<CollectionSourceBean> dataStream = env.fromCollection(Arrays.asList(
                new CollectionSourceBean("source_1", 1542334432L, 12.31),
                new CollectionSourceBean("source_2", 1542334445L, 29.8),
                new CollectionSourceBean("source_3", 1542334456L, 92.3),
                new CollectionSourceBean("source_4", 1542334467L, 21.2),
                new CollectionSourceBean("source_5", 1542334469L, 29.2),
                new CollectionSourceBean("source_6", 1542334472L, 83.2),
                new CollectionSourceBean("source_7", 1542334478L, 23.3),
                new CollectionSourceBean("source_8", 1542334492L, 61.5),
                new CollectionSourceBean("source_9", 1542334521L, 72.1),
                new CollectionSourceBean("source_10", 1542334527L, 81.7)
        ));

        // 从元素获取数据
        DataStream<Integer> integerDataStream = env.fromElements(Integer.class, 2, 5, 3, 1, 2, 7, 8, 4, 3, 7);

        // 打印
        dataStream.print("data");
        integerDataStream.print("integer")
                .setParallelism(1);

        // 执行
        env.execute();
    }
}
