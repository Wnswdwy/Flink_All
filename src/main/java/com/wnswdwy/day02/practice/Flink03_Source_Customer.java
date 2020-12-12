package com.wnswdwy.day02.practice;

import bean.SensorReading;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

/**
 * @author yycstart
 * @create 2020-12-11 10:45
 */
public class Flink03_Source_Customer {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 自定义
        DataStreamSource<SensorReading> source = env.addSource(new MySource());

        //3. 打印
        source.print();

        //4. 启动
        env.execute();



    }
    public static class MySource implements SourceFunction<SensorReading>{
        // 标志任务运行
        private boolean running = true;

        //定义随机数
        private Random random = new Random();

        //定义基本温度
        private Map<String,SensorReading> map = new HashedMap();


        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            for (int i = 0; i < 10; i++) {
                String id = "sensor_" + (i + 1);
                map.put(id,new SensorReading(id,System.currentTimeMillis(),60D + random.nextGaussian()*20));
            }
            while (running){
                //制造数据
                for (String id : map.keySet()) {
                    //写出数据
                    //读取上一次温度
                    SensorReading sensorReading = map.get(id);
                    Double lastTemp = sensorReading.getTemp();
                    sourceContext.collect(new SensorReading(id,System.currentTimeMillis(),lastTemp+random.nextGaussian()));
                }
                Thread.sleep(2000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
