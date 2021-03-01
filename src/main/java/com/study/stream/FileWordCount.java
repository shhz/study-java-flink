package com.study.stream;

import com.study.util.StringCountFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: StreamWordCount 数据流单词技术
 * @date 2021/3/1 16:21
 */
public class FileWordCount {
    public static void main(String[] args) throws Exception{
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设定此任务为 4 solt
        env.setParallelism(4);

        // 文件读取数据
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\words";
        DataStream<String> dataStream = env.readTextFile(filePath);

        // 数据流单词技术
        DataStream<Tuple2<String, Integer>> sum = dataStream.flatMap(new StringCountFlatMapFunction())
                .keyBy(0)
                .sum(1)
                .setParallelism(8); // 设定此步骤为 8 solt

        // 统计结果输出到控制台
        sum.print();

        // 数据流监听执行
        env.execute();
    }
}
