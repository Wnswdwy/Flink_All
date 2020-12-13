package com.wnswdwy.day03.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-13 21:26
 */
public class Flink04_Window_TimeSlide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapResult = socketTextStream.flatMap(new MyFlatMapFunc());

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMapResult.keyBy(0);

        ////5.开窗

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5), Time.seconds(2));



        windowedStream.sum(1).print();


        env.execute();


    }
    public static class MyFlatMapFunc implements FlatMapFunction<String,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}
