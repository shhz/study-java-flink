package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: DataStreamTest_Base
 * @date 2021/3/11 17:20
 */
public class DataStreamTest_Base {
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

        // 下游广播
        dataStream.broadcast();
        // 洗牌（随机向下游发送数据）
        dataStream.shuffle();
        // 直通（只向当前分区传输）
        dataStream.forward();
        // 轮询方式向下游均匀分布数据（并行度不一样默认选择此方式分配任务）
        dataStream.rebalance();
        // 下游分组（组数与本节点并行度一致），针对组内下游节点轮询发送
        dataStream.rescale();
        // 全部推向下游的第一个实例（下游并行度为 1）
        dataStream.global();
        // 自定义重分区方式
        dataStream.partitionCustom(new MyPartitioner(), new MyKeySelector());

        // 打印
        dataStream.print();

        // 执行
        env.execute();
    }

    // 自定义分区器
    private static class MyPartitioner implements Partitioner<String> {

        @Override
        public int partition(String key, int numPartitions) {
            System.out.println("key:" + key);
            System.out.println("numPartitions:" + numPartitions);
            return numPartitions;
        }
    }

    // 键选择器
    private static class MyKeySelector implements KeySelector<CollectionSourceBean, String> {

        @Override
        public String getKey(CollectionSourceBean value) throws Exception {
            return value.getId();
        }
    }
}
