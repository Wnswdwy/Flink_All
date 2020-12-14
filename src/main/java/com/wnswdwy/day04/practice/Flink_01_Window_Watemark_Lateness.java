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
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-14 15:35
 *
 * 乱序
 *
 */
public class Flink_01_Window_Watemark_Lateness {
    public static void main(String[] args) throws Exception {
        //1. 获取连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //引入事件事件定义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2. 从端口获取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3. 指定数据中的事件字段
        SingleOutputStreamOperator<String> timestampsAndWatermarks = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {

            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1] )* 1000L;
            }
        });

        //4. 压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = timestampsAndWatermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0],1);
            }
        });
        //5. 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = operator.keyBy(0);

        //6. 开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> outputLateData = keyedStream
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
                });
        //6. 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sunResult = outputLateData.sum(1);

        //7. 打印
        sunResult.print("sumResult");

        //8. 获取并输出数据
        sunResult.getSideOutput(new OutputTag<Tuple2<String,Integer>>("sideOutPut"){}).print("sideOutPut");

        // 执行
        env.execute();
    }
}
