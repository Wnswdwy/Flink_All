package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.xml.bind.ValidationEvent;
import java.util.Collections;

/**
 * @author yycstart
 * @create 2020-12-11 21:08
 */
public class Flink17_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> senorReadFile = env.readTextFile("senor");

        SingleOutputStreamOperator<SensorReading> mapResult = senorReadFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        SplitStream<SensorReading> splitResult = mapResult.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });

        DataStream<SensorReading> highResult = splitResult.select("high");
        DataStream<SensorReading> lowResult = splitResult.select("low");


        DataStream<SensorReading> unionResult = highResult.union(lowResult);
        SingleOutputStreamOperator<Object> finalResult = unionResult.map(new MapFunction<SensorReading, Object>() {
            @Override
            public Object map(SensorReading value) throws Exception {
                return value.getId() + "=>" + value.getTs() + "=>" + value.getTemp();
            }
        });

        finalResult.print();


        //启动
        env.execute();

    }
}
