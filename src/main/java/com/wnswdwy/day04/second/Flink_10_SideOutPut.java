package com.wnswdwy.day04.second;

import bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-17 16:11
 */
public class Flink_10_SideOutPut {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //2. 从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3.将数据转换成JavaBean
        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        //4.按照Key进行分组
        KeyedStream<SensorReading, String> keyedStream = mapResult.keyBy(SensorReading::getId);
        //5。两次温差大于10.0，报警，将报警信息输出到测输出流
        SingleOutputStreamOperator<Tuple2<String, Object>> process = keyedStream.process(new MyKedProceFunc(10.0));

        process.print("Main");

        process.getSideOutput(new OutputTag<Tuple2<String,Object>>("Error"){}).print("SideOut");


        env.execute();

    }
    public static class MyKedProceFunc extends KeyedProcessFunction<String,SensorReading, Tuple2<String,Object>>{
        ValueState<Double> tempState;
        private Double diffTemp;

        public MyKedProceFunc(Double diffTemp) {
            this.diffTemp = diffTemp;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class,0.0));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<Tuple2<String, Object>> out) throws Exception {
            Double curTemp = value.getTemp();
            Double lastTemp = tempState.value();
            double diff = Math.abs(lastTemp - curTemp);
            tempState.update(curTemp);
            if(curTemp > lastTemp || lastTemp == null){
                out.collect(new Tuple2<>(value.getId(),value.getTemp()));
            }else {
                ctx.output(new OutputTag<Tuple2<String,Object>>("Error"){},new Tuple2<>("异常","温度降低了"+ diff));
            }

        }
    }
}
