package com.study.wc;

import com.study.map.function.StringCountFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author shsq
 * @description: WordCount 单词计数
 * @date 2021/3/1 15:52
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 文件中读取数据
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\words";
        DataSet<String> stringDataSet = env.readTextFile(filePath);

        // 计数操作 以空格分隔 转换为 (word,1) 计算统计数
        DataSet<Tuple2<String, Integer>> sumDataSet = stringDataSet.flatMap(new StringCountFlatMapFunction())
                .groupBy(0)
                .sum(1);

        // 结果打印到控制台
        sumDataSet.print();
    }
}
