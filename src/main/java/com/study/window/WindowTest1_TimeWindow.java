package com.study.window;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;

/**
 * @author shsq
 * @description: WindowTest1_TimeWindow
 * @date 2021/3/18 16:30
 */
public class WindowTest1_TimeWindow {
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

        // 开窗测试
        // 增量聚合
        DataStream<Integer> resultStream = dataStream.keyBy(CollectionSourceBean::getId)
                // 滚动窗口单参数，滑动窗口两个参数
//                .countWindow(10, 2)
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
                .timeWindow(Time.seconds(15), Time.seconds(5))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15L)));
                .aggregate(new AggregateFunction<CollectionSourceBean, Integer, Integer>() {

                    // 创建累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 累加操作
                    @Override
                    public Integer add(CollectionSourceBean value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 获取结果
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 合并
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        OutputTag outPutTag = new OutputTag<CollectionSourceBean>("late");

        // 全窗口聚合
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> resultStream2 =
                dataStream.keyBy(CollectionSourceBean::getId)
                        .timeWindow(Time.seconds(15), Time.seconds(5))
                        // 等待一分钟再关窗
                        .allowedLateness(Time.minutes(1L))
                        // 再迟的数据就放入侧输出流
                        .sideOutputLateData(outPutTag)
                        .apply(new WindowFunction<CollectionSourceBean, Tuple3<String, String, Integer>, String, TimeWindow>() {
                            @Override
                            public void apply(String s, TimeWindow window, Iterable<CollectionSourceBean> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                                String windowString = window.toString() + "_" + window.hashCode();
                                Integer count = IteratorUtils.toList(input.iterator()).size();
                                out.collect(new Tuple3<>(s, windowString, count));
                            }
                        });
//                .countWindow(10, 2)
//                .process(new ProcessWindowFunction<CollectionSourceBean, Object, String, GlobalWindow>() {
//                })
//                .apply(new WindowFunction<CollectionSourceBean, Tuple3<String, String, Integer>, String, GlobalWindow>() {
//                    @Override
//                    public void apply(String id, GlobalWindow window,
//                                      Iterable<CollectionSourceBean> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
//                        String windowString = window.toString() + "_" + window.hashCode();
//                        Integer count = IteratorUtils.toList(input.iterator()).size();
//                        out.collect(new Tuple3<>(id, windowString, count));
//                    }
//                });

//        resultStream.print();
        resultStream2.getSideOutput(outPutTag).print(outPutTag.getId());
        resultStream2.print();

        // 执行
        env.execute();
    }
}
