package com.wnswdwy.day04.practice;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author yycstart
 * @create 2020-12-14 22:17
 */
public class Flink_10_ProcessAPI_SideOutPut {
    public static void main(String[] args) throws Exception {
        //1. 获取连接
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 从socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3. 分组
        SingleOutputStreamOperator<String> keyedResult = socketTextStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).process(new MySideOutPutProcessFunc());

        //4.
        keyedResult.print("high");

        DataStream<Tuple2<String, Double>> sideOutput = keyedResult.getSideOutput(new OutputTag<Tuple2<String, Double>>("lowTag") {
        });
        //提取侧输出流并打印
        sideOutput.print("low");

        env.execute();
    }

    ////使用ProcessAPI实现高低温分流操作
    public static class MySideOutPutProcessFunc extends KeyedProcessFunction<String,String,String>{
        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

            String[] fields = value.split(",");
            double temp = Double.parseDouble(fields[2]);
            //高温数据。写到主流
            if (temp > 30.0){
                out.collect(value);
            }else {
                //低温数据,输出到侧输出流
                ctx.output(new OutputTag<Tuple2<String,Double>>("lowTag"){},new Tuple2<>(fields[0],temp));
            }
        }
    }
}
