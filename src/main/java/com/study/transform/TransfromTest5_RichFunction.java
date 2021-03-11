package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author shsq
 * @description: TransfromTest5_RichFunction
 * @date 2021/3/11 16:45
 */
public class TransfromTest5_RichFunction {
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

        // 转换
        DataStream<Tuple2<String, Integer>> resultStream = dataStream.map(new MyRichMapper());

        // 打印
        resultStream.print();

        // 执行
        env.execute();
    }

    public static class MyMapper implements MapFunction<CollectionSourceBean, Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> map(CollectionSourceBean value) throws Exception {
            return new Tuple2<>(value.getId(), value.getId().length());
        }
    }

    // 富函数
    public static class MyRichMapper extends RichMapFunction<CollectionSourceBean, Tuple2<String, Integer>> {
        // 运行方法
        @Override
        public Tuple2<String, Integer> map(CollectionSourceBean value) throws Exception {
            // 可以获取运行时上下文
            RuntimeContext runtimeContext = getRuntimeContext();
            return new Tuple2<>(value.getId(), runtimeContext.getIndexOfThisSubtask());
        }

        // 初始化方法
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open_" + getRuntimeContext().getIndexOfThisSubtask());
        }

        // 销毁方法
        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close_" + getRuntimeContext().getIndexOfThisSubtask());
        }
    }
}
