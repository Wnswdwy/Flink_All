package com.wnswdwy.day02.second;




import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author yycstart
 * @create 2020-12-11 18:59
 */
public class Flink07_Source_Consumer {
    public static void main(String[] args) throws Exception {
        //1. 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从自定义数据源获取数据
        DataStreamSource<SensorReading> source = env.addSource(new MySource());
        //3. 打印
        source.print();
        //4. 启动
        env.execute();
    }

    public static class MySource implements SourceFunction<SensorReading>{
        //定义标记,标记任务状态
        private boolean isFlag = true;
        //定义随机数
        private Random random = new Random();
        //定义基础温度
        private Map<String,SensorReading> map = new HashMap<>();

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            for (int i = 0; i < 10; i++) {
                String id = "senor_" + i;
                map.put(id,new SensorReading(id,System.currentTimeMillis(),(60+random.nextGaussian()*20)));
            }
            while (isFlag){
                //造数据
                for (String id : map.keySet()) {
                    //写出数据
                    //获取上一次数据
                    SensorReading sensorReading = map.get(id);
                    Double lastTemp = sensorReading.getTemp();
                    SensorReading reading = map.put(id, new SensorReading(id, System.currentTimeMillis(), lastTemp + random.nextGaussian() * 20));

                    //写出
                    sourceContext.collect(reading);

                    //更新map数据
                    map.put(id,reading);

                }
                System.out.println("准备休息一下....");
                Thread.sleep(2000);
                System.out.println("睡眠结束....");

            }
        }

        @Override
        public void cancel() {
            isFlag = false;
        }
    }
}
