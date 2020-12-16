package com.wnswdwy.day04.practice;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yycstart
 * @create 2020-12-14 23:06
 *
 * 3.使用RichFunction实现状态编程,如果同一个传感器连续两次温度差值超过10度,则输出报警信息
 */
public class Flink_12_1_State_ProcessAPI_TempDiff {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));

            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = mapResult.keyBy("id");

        keyedStream.map(new MyTempDiffFunc(10.0));

        env.execute();
    }

    public static class MyTempDiffFunc extends RichMapFunction<SensorReading, String> {

        ValueState<Double> valueState;
        private Double maxDiff;

        public MyTempDiffFunc(Double maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("Temp-Diff", Double.class));

        }

        @Override
        public String map(SensorReading value) throws Exception {
            //获取上一次
            Double lastTemp = valueState.value();
            //更新
            valueState.update(value.getTemp());
            //判断
            if(!(lastTemp == null) && (Math.abs(lastTemp - value.getTemp()) > maxDiff ) ){
                return "两次温度差已经达到"+ maxDiff+ "度";
            }
                valueState.update(value.getTemp());

            return "";
        }
    }
}
