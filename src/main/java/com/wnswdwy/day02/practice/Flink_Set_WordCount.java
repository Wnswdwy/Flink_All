package com.wnswdwy.day02.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.HashSet;

/**
 * @author yycstart
 * @create 2020-12-12 9:23
 */
public class Flink_Set_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> input = env.readTextFile("input");

        SingleOutputStreamOperator<String> flatMap = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {

                    out.collect(word);
                }
            }
        });

        flatMap.filter(new FilterFunction<String>() {
            HashSet set;
            @Override
            public boolean filter(String value) throws Exception {
                boolean contains = set.contains(value);
                if (!contains) {
                    set.add(value);
                    return true;
                }
                return false;
            }
        }).print();

        env.execute();

    }
}
