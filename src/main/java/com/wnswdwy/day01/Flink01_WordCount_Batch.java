package com.wnswdwy.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-09 10:47
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {

        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



        // 从文件中读取数据
        String inputPath = "D:\\Z_Myself\\input\\";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 空格分词打散之后，对单词进行groupby分组，然后用sum进行聚合
        DataSet<Tuple2<String, Integer>> wordCountDataSet =
                inputDataSet.flatMap(new MyFlatMapper())
                        .groupBy(0)
                        .sum(1);

        // 打印输出
        wordCountDataSet.print();
    }

    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}

