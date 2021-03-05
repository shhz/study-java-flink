package com.study.map.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author shsq
 * @description: StringCountFlatMapFunction
 * @date 2021/3/1 16:13
 */
public class StringCountFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
    public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
        if(StringUtils.isNotBlank(s)){
            String[] words = s.split(" ");
            for (String word : words) {
                out.collect(new Tuple2(word, 1));
            }
        }
    }
}
