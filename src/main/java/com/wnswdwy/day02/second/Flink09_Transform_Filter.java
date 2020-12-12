package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-11 10:45
 */
public class Flink09_Transform_Filter {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2. 获取数据
        DataStreamSource<String> input = env.readTextFile("senor");
        //3.Filter过滤掉温度大于30的
        SingleOutputStreamOperator<String> filerResult = input.filter(in -> {
            String[] split = in.split(",");
            return !(Double.parseDouble(split[2]) > 30);
        });
        //4.打印
        filerResult.print();
        //5. 启动
        env.execute();

    }
}
