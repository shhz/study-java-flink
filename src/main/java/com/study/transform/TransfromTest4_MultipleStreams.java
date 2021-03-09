package com.study.transform;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author shsq
 * @description: TransfromTest4_MultipleStreams 多流转换测试
 * @date 2021/3/9 14:29
 */
public class TransfromTest4_MultipleStreams {
    public static void main(String[] args) throws Exception {
        // 建立流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 分流 按照温度值40度为界分为两条流
        SplitStream<CollectionSourceBean> split = dataStream.split(sourceBean -> {
            return (sourceBean.getTemperature() > 40) ?
                    Collections.singletonList("high") : Collections.singletonList("low");
        });

        // 分拣数据并打印
        DataStream<CollectionSourceBean> highStream = split.select("high");
        DataStream<CollectionSourceBean> lowStream = split.select("low");
        DataStream<CollectionSourceBean> allStream = split.select("high", "low");

        // 打印数据
        highStream.print("high");
        lowStream.print("low");
        allStream.print("all");

        // 合流 connect 将高温流转换为二元数据类型（id，温度），与低温流连接合并后，输出状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningSteam = highStream.map(
                new MapFunction<CollectionSourceBean, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(CollectionSourceBean value) throws Exception {
                        return new Tuple2<String, Double>(value.getId(), value.getTemperature());
                    }
                });

        // 连接 此方法只能二合一但是可以合并不同数据类型的流
        ConnectedStreams<Tuple2<String, Double>, CollectionSourceBean> connect = warningSteam.connect(lowStream);
        DataStream<Object> resultStream = connect.map(
                new CoMapFunction<Tuple2<String, Double>, CollectionSourceBean, Object>() {
                    @Override
                    public Object map1(Tuple2<String, Double> value) throws Exception {
                        return new Tuple3<String, Double, String>(value.f0, value.f1, "high temp waring!");
                    }

                    @Override
                    public Object map2(CollectionSourceBean value) throws Exception {
                        return new Tuple2<>(value.getId(), "normal");
                    }
                });

        // 同数据类型的多流合并
        DataStream<CollectionSourceBean> union = highStream.union(lowStream, allStream);

        // 打印合流后数据
        resultStream.print("connect");
        union.print("union");

        // 执行
        env.execute();
    }
}
