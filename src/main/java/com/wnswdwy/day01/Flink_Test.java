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
 * @create 2020-12-09 22:43
 */
public class Flink_Test {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> input = executionEnvironment.readTextFile("input");
        FlatMapOperator<String, Tuple2<String, Integer>> stringTuple2FlatMapOperator = input.flatMap(new MyFlatMapFunc());
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = stringTuple2FlatMapOperator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        result.print();


    }
    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] strings = s.split(" ");
            for (String str : strings) {
                collector.collect(new Tuple2(str,1));
            }
        }
    }
}
