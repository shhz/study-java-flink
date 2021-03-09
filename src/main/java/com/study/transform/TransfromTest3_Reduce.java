package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: TransfromTest3_Reduce 聚合测试
 * @date 2021/3/5 16:35
 */
public class TransfromTest3_Reduce {
    public static void main(String[] args) throws Exception {
        // 建立环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        String filePath = "D:\\data\\gitStudyData\\study_java_flink\\src\\main\\resources\\SourceFile";
        DataStream<String> inputStream = env.readTextFile(filePath);

        // 转换为实体类
        DataStream<CollectionSourceBean> dataStream = inputStream.map(line -> {
            if (StringUtils.isBlank(line)) {
                return new CollectionSourceBean("null", 0L, 0D);
            }
            String[] strings = line.split(",");
            return new CollectionSourceBean(strings[0], Long.valueOf(strings[1]), Double.valueOf(strings[2]));
        });

        // 分组
        KeyedStream<CollectionSourceBean, String> keyedStream = dataStream.keyBy(CollectionSourceBean::getId);

        // reduce 取最大的温度值，以及当前最新的时间戳
        DataStream<CollectionSourceBean> resultStream = keyedStream.reduce((curState, newData) -> {
            // 返还（已知最大的id，新比较数据的时间戳，已知最大和最新数据中最大的温度）
            return new CollectionSourceBean(curState.getId(), newData.getTimestamp(),
                    Math.max(curState.getTemperature(), newData.getTemperature()));
        });
        /* 与上一段代码等价
        DataStream<CollectionSourceBean> resultStream =
                keyedStream.reduce(new ReduceFunction<CollectionSourceBean>() {
                    @Override
                    public CollectionSourceBean reduce(CollectionSourceBean value1, CollectionSourceBean value2)
                            throws Exception {
                        return new CollectionSourceBean(value1.getId(), value2.getTimestamp(),
                                Math.max(value1.getTemperature(), value2.getTemperature()));
                    }
                });*/

        // 打印
        resultStream.print();

        // 执行
        env.execute();
    }
}
