package com.study.window;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author shsq
 * @description: WindowTest2_CountWindows
 * @date 2021/3/23 16:15
 */
public class WindowTest2_CountWindows {
    public static void main(String[] args) throws Exception {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
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

        // 开计数窗口测试
        SingleOutputStreamOperator<Double> avgResultStream = dataStream.keyBy(CollectionSourceBean::getId)
                .countWindow(10, 2)
                .aggregate(new MyAvgTemp());

        // 全窗口聚合
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> avgResultStream2 =
                dataStream.keyBy(CollectionSourceBean::getId)
                .countWindow(10, 2)
                .apply(new WindowFunction<CollectionSourceBean, Tuple3<String, String, Integer>, String, GlobalWindow>() {
                    @Override
                    public void apply(String id, GlobalWindow window,
                                      Iterable<CollectionSourceBean> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        String windowString = window.toString() + "_" + window.hashCode();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowString, count));
                    }
                });

        // 打印
        avgResultStream.print("avgResultStream");
        avgResultStream2.print("avgResultStream2");

        // 执行
        env.execute();
    }

    public static class MyAvgTemp implements AggregateFunction<CollectionSourceBean, Tuple2<Double, Integer>, Double> {

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0D, 0);
        }

        @Override
        public Tuple2<Double, Integer> add(CollectionSourceBean value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
        }
    }
}
