package com.wnswdwy.day04.practice;

import com.wnswdwy.day03.practice.Flink04_Window_TimeSlide;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-14 19:50
 */
public class Flink_04_Window_Watermark_TimeSession {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<String> operator = env.socketTextStream("hadoop102", 9999).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                return Long.parseLong(element.split(",")[1]) * 1000L;
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = operator.flatMap(new Flink04_Window_TimeSlide.MyFlatMapFunc());


        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));


        window.sum(1).print();

        env.execute();
    }
}
