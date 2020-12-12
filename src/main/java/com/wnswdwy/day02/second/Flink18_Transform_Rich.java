package com.wnswdwy.day02.second;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-11 21:19
 */
public class Flink18_Transform_Rich {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> readResult = env.readTextFile("input");


        SingleOutputStreamOperator<Tuple2<String, Integer>> operatorResult = readResult.flatMap(new MyRichFlatMap());

        operatorResult.keyBy(0).sum(1).print();


        //启动
        env.execute();

    }
    public static class MyRichFlatMap extends RichFlatMapFunction<String,Tuple2<String,Integer>>{

        //生命周期



        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            RuntimeContext runtimeContext = getRuntimeContext();
            runtimeContext.getState(new ValueStateDescriptor<Object>(" ",Object.class));
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
        @Override
        public void close() throws Exception {
            super.close();
        }

    }
}
