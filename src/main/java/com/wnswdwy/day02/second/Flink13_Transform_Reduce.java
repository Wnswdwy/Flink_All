package com.wnswdwy.day02.second;

import bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yycstart
 * @create 2020-12-11 20:24
 */
public class Flink13_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setParallelism(2);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(in -> {
            String[] fields = in.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        KeyedStream<SensorReading, Tuple> keyByResult = mapResult.keyBy("id");


        SingleOutputStreamOperator<SensorReading> reduceResult = keyByResult.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {

                return new SensorReading(value1.getId(), value2.getTs(), Math.max(value1.getTemp(), value2.getTemp()));
            }
        });

        reduceResult.print();

        //启动
        env.execute();
    }
}
