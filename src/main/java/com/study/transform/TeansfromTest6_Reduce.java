package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: TeansfromTest6_Reduce
 * @date 2021/3/11 17:33
 */
public class TeansfromTest6_Reduce {
    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 转换数据类型
        DataStream<CollectionSourceBean> dataStream = inputStream.map(line -> {
            if (StringUtils.isBlank(line)) {
                return new CollectionSourceBean("null", 0L, 0D);
            }
            String[] ss = line.split(",");
            return new CollectionSourceBean(ss[0], Long.valueOf(ss[1]), Double.valueOf(ss[2]));
        });

        // 打印
        dataStream.print("data");

        // 重分区
        DataStream<CollectionSourceBean> shuffleStream = dataStream.shuffle();

        // 打印
        shuffleStream.print("shuffle");

        // keyBy 按照关键字重分区
        dataStream.keyBy(CollectionSourceBean::getId).print("keyBy");

        // global 全部分发到第一个分区
        dataStream.global().print("global");

        // 执行
        env.execute();
    }
}
