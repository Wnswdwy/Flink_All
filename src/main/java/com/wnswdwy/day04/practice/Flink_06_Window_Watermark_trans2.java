package com.wnswdwy.day04.practice;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import sun.management.Sensor;

/**
 * @author yycstart
 * @create 2020-12-14 20:30
 */
public class Flink_06_Window_Watermark_trans2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> operator = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });
        SingleOutputStreamOperator<SensorReading> assignTimestampsAndWatermarks = operator.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });
        KeyedStream<SensorReading, Tuple> keyedStream = assignTimestampsAndWatermarks.keyBy("id");

        WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(6));

        windowedStream.sum("temp").print();

        env.execute();

    }
}
