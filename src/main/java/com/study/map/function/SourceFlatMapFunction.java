package com.study.map.function;

import com.study.beans.CollectionSourceBean;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author shsq
 * @description: SourceFlatMapFunction
 * @date 2021/3/5 11:20
 */
public class SourceFlatMapFunction implements FlatMapFunction<String, CollectionSourceBean> {
    @Override
    public void flatMap(String s, Collector<CollectionSourceBean> collector) throws Exception {
        String ss[] = s.split(",");
        collector.collect(new CollectionSourceBean(ss[0], Long.valueOf(ss[1]), Double.valueOf(ss[2])));
    }
}
