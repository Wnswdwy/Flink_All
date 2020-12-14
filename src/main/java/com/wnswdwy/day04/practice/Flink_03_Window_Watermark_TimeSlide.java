package com.wnswdwy.day04.practice;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author yycstart
 * @create 2020-12-14 19:42
 */
public class Flink_03_Window_Watermark_TimeSlide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> operator = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {

            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = operator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");

                return new Tuple2<>(fields[0], 1);
            }
        });


        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);


        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(6), Time.seconds(2));


        SingleOutputStreamOperator<Tuple2<String, Integer>> result = timeWindow.sum(1);


        result.print();


        env.execute();

    }
}
