package com.study.source;

import com.study.beans.CollectionSourceBean;
import com.study.map.function.SourceFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author shsq
 * @description: KafkaSourceTest
 * @date 2021/3/5 11:31
 */
public class KafkaSourceTest {
    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // kafka 配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.serves", "localhost:9091");
        properties.setProperty("group.id", "source-test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // 数据源
        DataStream<CollectionSourceBean> dataStream = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "source-test",
                        new SimpleStringSchema(),
                        properties
                )).flatMap(new SourceFlatMapFunction());

        // 打印
        dataStream.print();

        // 执行
        env.execute();
    }
}
