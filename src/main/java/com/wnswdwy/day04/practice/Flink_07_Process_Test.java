package com.wnswdwy.day04.practice;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author yycstart
 * @create 2020-12-14 20:44
 */
public class Flink_07_Process_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);


        SingleOutputStreamOperator<String> process = socketTextStream.process(new MyProcessFunc());

        process.getSideOutput(new OutputTag<>("outPutStage")).print();

        env.execute();


    }

    public static class MyProcessFunc extends ProcessFunction<String,String>{



        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            RuntimeContext runtimeContext = getRuntimeContext();
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            long l = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(1L);
            ctx.timerService().deleteProcessingTimeTimer(1L);


            ctx.timerService().currentWatermark();
            ctx.timerService().deleteEventTimeTimer(1L);
            ctx.timerService().registerEventTimeTimer(1L);


            out.collect("");
            ctx.output(new OutputTag<>("outPutStage"),"");


        }
    }
}
