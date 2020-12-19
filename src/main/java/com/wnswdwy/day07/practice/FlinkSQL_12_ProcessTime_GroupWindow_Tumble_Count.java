package com.wnswdwy.day07.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-18 20:22
 *
 * processTime中计数没有SQL的写法
 */
public class FlinkSQL_12_ProcessTime_GroupWindow_Tumble_Count {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //端口读取数据并转换成JavaBean，添加延迟时间
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTs() * 1000L;
                    }
                });
        //3.将流转换为表并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS,"id,ts,temp,pt.proctime");
        //4.基于时间的滚动窗口TableAPI
        Table tableResult = table.window(Tumble.over("5.rows").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count");
        //5.转换为流进行输出
        tableEnv.toAppendStream(tableResult, Row.class).print();
        //6.执行
        env.execute();

    }
}
