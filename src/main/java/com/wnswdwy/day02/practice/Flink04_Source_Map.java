package com.wnswdwy.day02.practice;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yycstart
 * @create 2020-12-11 10:45
 */
public class Flink04_Source_Map {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2. 从collect获取数据
        DataStreamSource<String> input = env.readTextFile("senor");
        SingleOutputStreamOperator<Object> result = input.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                String[] split = s.split(",");
                return split[0] + "--" + split[1] + "=====" + split[2];
            }
        });
        //3. 打印
        result.print();
        //4. 执行
        env.execute();



    }
}
