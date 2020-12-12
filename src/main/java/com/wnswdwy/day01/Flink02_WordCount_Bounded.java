package com.wnswdwy.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * @author yycstart
 * @create 2020-12-09 10:47
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1. 获取Flink连接
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. 读取文本数据
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("input");

        //3. 压平操作 （hello,1）(at,1)...
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new MyFlatMapFunc());
        //4. 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tuple2SingleOutputStreamOperator.keyBy(0);
        //5. 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = tuple2TupleKeyedStream.sum(1);
        //6. 打印
        sumResult.print();
    }
        public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //按照空格切分
                String[] words = value.split(" ");
                //遍历写出
                for (String word : words) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }

    }


}