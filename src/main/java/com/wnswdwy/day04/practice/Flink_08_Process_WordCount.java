package com.wnswdwy.day04.practice;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author yycstart
 * @create 2020-12-14 20:55
 */
public class Flink_08_Process_WordCount {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 从socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //3. 压平
        SingleOutputStreamOperator<String> processFlatMap = socketTextStream.process(new MyProcessFlatMapFun());
        //4. 将String =》Tuple2<String,Integer>
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapProcess = processFlatMap.process(new MyMapProcessFunc());
        //5. 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = mapProcess.keyBy(0);
        //6.聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumProcess = keyedStream.process(new MySumProcessFunc());
        //7. 打印
        sumProcess.print();

        //启动
        env.execute();
    }

    /**
     * 压平
     */
    public static class MyProcessFlatMapFun extends ProcessFunction<String, String> {


        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(word);
            }

        }
    }
    public static class MyMapProcessFunc extends ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }

    public static class MySumProcessFunc extends KeyedProcessFunction<Tuple,Tuple2<String,Integer>,Tuple2<String,Integer>>{
        private Integer count = 0;
        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value.f0,++count));
        }
    }
}
