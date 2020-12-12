package com.wnswdwy.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-09 23:01
 */
public class Flink_test02 {
    public static void main(String[] args) throws Exception {
        //获取连接
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置读取路径
        DataStreamSource<String> input = environment.readTextFile("input");

        //压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = input.flatMap(new MyFlatMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByResult = flatMap.keyBy(0);

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = keyByResult.sum(1);

        //打印
        sumResult.print();

        environment.execute();
    }
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
