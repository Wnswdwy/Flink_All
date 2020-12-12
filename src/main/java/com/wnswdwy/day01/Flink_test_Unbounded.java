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
 * @create 2020-12-10 9:16
 */
public class Flink_test_Unbounded {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = streamExecutionEnvironment.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapValue = source.flatMap(new MyFlatMapFunction());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByValue = flatMapValue.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumResult = keyByValue.sum(1);
        sumResult.print();

        streamExecutionEnvironment.execute();
    }
  public static class  MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

      @Override
      public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
          String[] words = s.split(" ");
          for (String word : words) {
              collector.collect(new Tuple2<>(word,1));
          }
      }
  }
}
