package com.study.sink;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author shsq
 * @description: SinkTest3_Es
 * @date 2021/3/15 16:23
 */
public class SinkTest3_Es {
    public static void main(String[] args) throws Exception {
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

        List<HttpHost> httoHosts = new ArrayList<>();
        httoHosts.add(new HttpHost("localhost", 9200));

        dataStream.addSink(
                new ElasticsearchSink.Builder<CollectionSourceBean>(
                        httoHosts, new MyEsSinkFunction()
                ).build());

        env.execute();
    }

    private static class MyEsSinkFunction implements ElasticsearchSinkFunction<CollectionSourceBean> {

        @Override
        public void process(CollectionSourceBean sourceBean, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
            // 定义写入的数据源
            HashMap<String, String> dataSource = new HashMap<>();
            dataSource.put("id", sourceBean.getId());
            dataSource.put("temp", sourceBean.getTemperature().toString());
            dataSource.put("ts", sourceBean.getTimestamp().toString());

            // 创建请求
            Requests.indexRequest().index("sensor").type("reading_data").source(dataSource);
        }
    }
}
