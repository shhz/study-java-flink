package com.study.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author shsq
 * @description: TransfromTest 基本转换算子测试
 * @date 2021/3/5 15:33
 */
public class TransfromTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 1.map，输出文字长度
        DataStream<Integer> lengthMapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // 2. flatMap，按‘,’分隔
        DataStream<String> stringflatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] datas = s.split(",");
                for (String data : datas) {
                    collector.collect(data);
                }
            }
        });

        // 3. filter，筛选 source_1 开头的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("source_1");
            }
        });

        // 打印输出
        inputStream.print("input");
        lengthMapStream.print("lengthMap");
        stringflatMapStream.print("flatMap");
        filterStream.print("filter");

        // 执行
        env.execute();
    }
}
