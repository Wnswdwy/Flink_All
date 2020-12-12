package com.wnswdwy.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-10 9:01
 */
public class Flink_test_batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> input = batchEnvironment.readTextFile("input");
        FlatMapOperator<String, Tuple2<String, Integer>> flatMapResult = input.flatMap(new MyFunctionFlatMap());
        UnsortedGrouping<Tuple2<String, Integer>> groupByKeyResult = flatMapResult.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> sumResult = groupByKeyResult.sum(1);
        sumResult.print();
    }
    public static class MyFunctionFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}

