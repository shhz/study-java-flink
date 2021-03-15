package com.study.sink;

import com.study.beans.CollectionSourceBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author shsq
 * @description: SinkTest2_Redis
 * @date 2021/3/15 15:59
 */
public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception{
        // 创建环境
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

        // 定义 jedis 连接配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379).build();

        dataStream.addSink(new RedisSink<>(redisConfig, new MyRedisMapper()));

        env.execute();
    }

    // 自定义 RedisMapper
    private static class MyRedisMapper implements RedisMapper<CollectionSourceBean> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            // 执行的指令及表名
            return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
        }

        // 生命存储的键
        @Override
        public String getKeyFromData(CollectionSourceBean sourceBean) {
            return sourceBean.getId();
        }

        // 声明存储的值
        @Override
        public String getValueFromData(CollectionSourceBean sourceBean) {
            return sourceBean.getTemperature().toString();
        }
    }
}
