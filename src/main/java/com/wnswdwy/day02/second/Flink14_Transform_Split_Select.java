package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

/**
 * @author yycstart
 * @create 2020-12-11 20:34
 */
public class Flink14_Transform_Split_Select {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> readTextFile = env.readTextFile("senor");

        SingleOutputStreamOperator<SensorReading> mapResult = readTextFile.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] feilds = value.split(",");
                return new SensorReading(feilds[0], Long.parseLong(feilds[1]), Double.parseDouble(feilds[2]));
            }
        });

        SplitStream<SensorReading> result_Split_Select = mapResult.split(new OutputSelector<SensorReading>() {

            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");
            }
        });
        DataStream<SensorReading> high = result_Split_Select.select("high");
        DataStream<SensorReading> low = result_Split_Select.select("low");


        high.print("high=>");
        low.print("low=>");

        //qidon
        env.execute();


    }
}
