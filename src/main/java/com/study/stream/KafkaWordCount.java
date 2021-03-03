package com.study.stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author shsq
 * @description: KafkaWordCount
 * @date 2021/3/2 16:23
 */
public class KafkaWordCount {
    public static void main(String[] args) {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源默认参数
        String hostName = "localhost";
        Integer port = 7777;

        // 获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 判空
        if (StringUtils.isNotBlank(parameterTool.get("host"))){
            hostName = parameterTool.get("host");
        }
        if (StringUtils.isNotBlank(parameterTool.get("port"))){
            port = parameterTool.getInt("port");
        }

        // 数据源
//        env.addSource(SourceFunction)

        // 分组并算总和
        // 打印
        // 执行
    }
}
