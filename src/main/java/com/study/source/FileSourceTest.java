package com.study.source;

import com.study.beans.CollectionSourceBean;
import com.study.map.function.SourceFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: FileSourceTest
 * @date 2021/3/5 11:13
 */
public class FileSourceTest {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 文件获取数据
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
        DataStream<CollectionSourceBean> dataStream = env.readTextFile(filePath).flatMap(new SourceFlatMapFunction());

        // 打印
        dataStream.print();

        // 执行
        env.execute();
    }
}
