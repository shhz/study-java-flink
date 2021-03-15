package com.study.sink;

import com.study.beans.CollectionSourceBean;
import com.study.map.function.SourceFlatMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author shsq
 * @description: SinkTest1_Kafka
 * @date 2021/3/15 15:37
 */
public class SinkTest1_Kafka {
    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
//        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
//        DataStream<String> inputStream = env.readTextFile(filePath);

        // kafka 配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.serves", "localhost:9091");
        properties.setProperty("group.id", "source-test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // Kafka 数据源
        DataStream<String> inputStream = env.addSource(
                new FlinkKafkaConsumer011<String>(
                        "source_test",
                        new SimpleStringSchema(),
                        properties
                ));

        // 转换数据类型
        DataStream<String> dataStream = inputStream.map(line -> {
            if (StringUtils.isBlank(line)) {
                return new CollectionSourceBean("null", 0L, 0D).toString();
            }
            String[] ss = line.split(",");
            return new CollectionSourceBean(ss[0], Long.valueOf(ss[1]), Double.valueOf(ss[2])).toString();
        });

        dataStream.addSink(new FlinkKafkaProducer011<String>(
                "localhost:9092", "sink_test", new SimpleStringSchema()));

        env.execute();
    }
}
