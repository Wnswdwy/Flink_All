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
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-14 18:58
 *
 * //有序
 */
public class Flink_02_Window_Watermark_ASC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定事件
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2. 获取接口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.指定时间
        SingleOutputStreamOperator<String> operator = socketTextStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        //4. 压平处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = operator.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });

        //5. 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = streamOperator.keyBy(0);

        //6. 开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> sideOutPut = keyedStream.timeWindow(Time.seconds(6))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});

        //7. 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sideOutPut.sum(1);

        //8. 打印
        result.print("result");

        //9.获取并输出数据
        result.getSideOutput(new OutputTag<Tuple2<String,Integer>>("sideOutPut"){}).print("sideOutPut");

        //执行
        env.execute();
    }
}
