package com.wnswdwy.day07.practice;


import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-18 14:41
 *
 *
 *
 *  tumble(pt,interval '10' second)窗口信息
 *  tumble_end(pt,interval '10' second) 窗口结束时间
 *
 */
public class FlinkSQL_10_ProcessTime_GroupWindow_Slide {
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
        Table tableResult = table.window(Slide.over("2.seconds").every("5.seconds").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count,tw.start");

        //5. 基于时间滚动的SQL API
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id),tumble_end(pt,interval '10' second)" +
                " from sensor " +
                "group by id,tumble(pt,interval '10' second)");


        //6.转换成数据流输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult,Row.class).print("SQL");

        //7.执行
        env.execute();
    }
}