package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author yycstart
 * @create 2020-12-11 20:54
 */
public class Flink16_Trandsform_Connect_CoMap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> readTextFile = env.readTextFile("senor");

        SingleOutputStreamOperator<SensorReading> mapResult = readTextFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4.按照温度大小进行切分
        SplitStream<SensorReading> splitResult = mapResult.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> highResult = splitResult.select("high").map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemp());
            }
        });
        DataStream<SensorReading> lowResult = splitResult.select("low");

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connect = highResult.connect(lowResult);


        SingleOutputStreamOperator<Object> connectResult = connect.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return stringDoubleTuple2;
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {
                return sensorReading;
            }
        });

        lowResult.print("lowResult");
        highResult.print("highResult");
        connectResult.print();
        //执行
        env.execute();
    }
}
