package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-11 10:45
 */
public class Flink08_Source_FlatMap {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2. 获取数据
        DataStreamSource<String> input = env.readTextFile("input");
        //3.压平，分组，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = input.flatMap(new MyFlatMapFunc()).keyBy(0).sum(1);
        //4. 打印
        sumResult.print();
        //5. 启动
        env.execute();


    }
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
