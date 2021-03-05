package com.study.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author shsq
 * @description: KafkaWordCount
 * @date 2021/3/2 16:23
 */
public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env = StreamExecutionEnvironment.createLocalEnvironment(1);
//        env = StreamExecutionEnvironment.createRemoteEnvironment("localHost", 12001);

        // 数据源默认参数
        String hostName = "localhost";
        Integer port = 7777;

        // 获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 判空
        if (StringUtils.isNotBlank(parameterTool.get("host"))) {
            hostName = parameterTool.get("host");
        }
        if (StringUtils.isNotBlank(parameterTool.get("port"))) {
            port = parameterTool.getInt("port");
        }

        // 建立 kafka 链接配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.serves", hostName + ":" + port);
        properties.setProperty("group.id", "source-test-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");


        // 数据源
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>(
                "word-count",
                new SimpleStringSchema(),
                properties
        ));

        // 分组并算总和
        DataStream<String> sum = dataStream.keyBy(0).sum(1);

        // 打印
        sum.print();

        // 执行
        env.execute();
    }
}
