package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: TransfromTest2_RollingAggregation 滚动聚合算子测试
 * @date 2021/3/5 15:49
 */
public class TransfromTest2_RollingAggregation {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 文件数据源
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 转换成实体类型
        DataStream<CollectionSourceBean> dataStream = inputStream.map(line -> {
            if (StringUtils.isBlank(line)) {
                return new CollectionSourceBean("null", 0L, 0D);
            }
            String[] strings = line.split(",");
            return new CollectionSourceBean(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
        });

        // 分组
//        KeyedStream<CollectionSourceBean, Tuple> keyedStream = dataStream.keyBy("id");
        KeyedStream<CollectionSourceBean, String> keyedStream = dataStream.keyBy(CollectionSourceBean::getId);

        // 滚动聚合，如果比较后原数据较大则会再一次输出原数据
        // max 只更新取最大值的字段
        DataStream<CollectionSourceBean> resultMaxStream = keyedStream.max("temperature");
        // maxBy 更新整行
        DataStream<CollectionSourceBean> resultMaxByStream = keyedStream.maxBy("temperature");


        // 打印结果
//        dataStream.print("data");
        resultMaxStream.print("max");
        resultMaxByStream.print("maxBy");

        // 执行
        env.execute();
    }
}
