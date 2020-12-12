package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yycstart
 * @create 2020-12-11 20:20
 */
public class Flink12_Transform_MaxBy {
        public static void main(String[] args) throws Exception {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);


            DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

            SingleOutputStreamOperator<SensorReading> senorResult = socketTextStream.map(new MapFunction<String, SensorReading>() {
                @Override
                public SensorReading map(String value) throws Exception {
                    String[] fields = value.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                }
            });

            //求出最大
            KeyedStream<SensorReading, Tuple> keyByResult = senorResult.keyBy("id");
            SingleOutputStreamOperator<SensorReading> max = keyByResult.maxBy("temp");


            max.print();

            env.execute();
        }
    }


