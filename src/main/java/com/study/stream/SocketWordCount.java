package com.study.stream;

import com.study.map.function.StringCountFlatMapFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: SocketWordCount
 * @date 2021/3/1 16:34
 */
public class SocketWordCount {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据源默认参数
        String hostName = "localhost";
        Integer port = 7777;

        // 获取启动参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        // 判空
        if (StringUtils.isNotBlank(parameterTool.get("host"))){
            hostName = parameterTool.get("host");
        }
        if (StringUtils.isNotBlank(parameterTool.get("port"))){
            port = parameterTool.getInt("port");
        }

        // socket 监听获取数据
        DataStream<String> inputDataStream = env.socketTextStream(hostName, port);

        // 单词计数
        DataStream<Tuple2<String, Integer>> sum = inputDataStream.flatMap(new StringCountFlatMapFunction())
                .keyBy(0)
                .sum(1);

        // 打印输入数据
        sum.print();

        // 启动
        env.execute();
    }

}
