package com.wnswdwy.day06.second;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-17 11:26
 */
public class FlinkSQL_01_Test {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2. 从端口获取数据并转成JavaBean
        SingleOutputStreamOperator<SensorReading> socketMapResult = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });
        //3. 注册环境
        tableEnv.createTemporaryView("sensor",socketMapResult);
        //4. SQL
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");
        //5.TableAPI
        Table table = tableEnv.fromDataStream(socketMapResult);
        Table tableResult = table.groupBy("id").select("id,temp.max");

        //6.将表转换为流进行输出.
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult,Row.class).print("sqlResult");

        //7. 执行
        env.execute();
    }
}
