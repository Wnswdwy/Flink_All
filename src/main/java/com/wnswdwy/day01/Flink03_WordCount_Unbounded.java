package com.wnswdwy.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-09 10:47
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从Socket中读取数据

        DataStreamSource<String> source = executionEnvironment.socketTextStream("hadoop102", 9999);

        // 空格分词打散之后，对单词进行groupby分组，然后用sum进行聚合

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = source.flatMap(new MyFlatMapper()).keyBy(0).sum(1).setParallelism(6);

        // 打印输出
        sumResult.print();
        //
        executionEnvironment.execute();

    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}

