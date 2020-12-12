package com.wnswdwy.day02.second;


import bean.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author yycstart
 * @create 2020-12-11 10:45
 */
public class Flink03_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从集合中获取数据

        DataStreamSource<SensorReading> senorDS = env.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)
        ));

        //3. 过滤掉temp大于30的数据

        SingleOutputStreamOperator<SensorReading> filterResult = senorDS.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return !(value.getTemp() > 30);
            }
        });

        //4. 打印
        filterResult.print();
        //5. 启动
        env.execute();
    }
}
