package evertday.practice;

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
 * @create 2020-12-15 8:50
 * 3.使用事件时间处理数据,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,
 * Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放至侧输出流,
 * 通过观察结果说明什么样的数据会进入侧输出流。
 *
 */
public class Flink_day05_Test03_EventTime {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<String> assignTimestampsAndWatermarks = socketTextStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapResult = assignTimestampsAndWatermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });


        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapResult.keyBy(0);


        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("outPutTage") {
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowedStream.sum(1);

        sum.print("Main=>");

        sum.getSideOutput(new OutputTag<>("outPutTage")).print("outPutTage=>");

        env.execute();
    }
}
