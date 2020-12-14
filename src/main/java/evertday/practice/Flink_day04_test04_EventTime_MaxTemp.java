package evertday.practice;



import bean.SensorReading;
import com.wnswdwy.day03.practice.Flink02_Sink_JDBC;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-14 8:45
 *
 *
 * 编写代码从端口获取数据实现每隔5秒钟计算最近30秒内每个传感器的最高温度(使用事件时间).
 *
 */
public class Flink_day04_test04_EventTime_MaxTemp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> outputStreamOperator = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");

                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = outputStreamOperator.keyBy("id");

        WindowedStream<SensorReading, Tuple, TimeWindow> timeWindow = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5));

        timeWindow.max("temp");


        env.execute();



    }
}
