package com.wnswdwy.day07.practice;


import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-18 14:41
 *
 *
 *
 *
 */
public class FlinkSQL_13_ProcessTime_GroupWindow_Slide_Count {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });
        //3.将数据流转换成表，并指定处理时间字段
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,pt.proctime");

        //4.基于时间的滚动窗口TableAPI
        Table tableResult = table.window(Slide.over("4.rows").every("2.rows").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count");



        //6.转换成数据流输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");

        //7.执行
        env.execute();
    }
}
