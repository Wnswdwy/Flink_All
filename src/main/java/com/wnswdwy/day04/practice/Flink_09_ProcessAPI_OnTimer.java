package com.wnswdwy.day04.practice;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-14 21:18
 *    //3.使用ProcessAPI测试定时器功能
 */
public class Flink_09_ProcessAPI_OnTimer {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3.使用ProcessAPI测试定时器功能
        SingleOutputStreamOperator<String> streamOperator = socketTextStream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        }).process(new MyKeyedProcessFun());

        //4.打印
        streamOperator.print();
        //5. 执行
        env.execute();
    }

 public static class MyKeyedProcessFun extends KeyedProcessFunction<String,String,String>{


     @Override
     public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
         out.collect(value);

         //注册2秒后的定时器
         ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime()+ 2000L);

     }
     @Override
     public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
         super.onTimer(timestamp, ctx, out);
         System.out.println("设定的定时器开始运作。。。。");
     }
 }

}
