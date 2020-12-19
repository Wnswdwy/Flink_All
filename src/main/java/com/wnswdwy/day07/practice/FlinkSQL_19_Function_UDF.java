package com.wnswdwy.day07.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

import java.io.DataInput;

/**
 * @author yycstart
 * @create 2020-12-18 23:18
 */
public class FlinkSQL_19_Function_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换成表
        Table table = tableEnv.fromDataStream(sensorDS);
        //4.注册函数
        tableEnv.registerFunction("mylen",new MyLenth());
        //5.用TableAPI
        Table tableResult = table.select("id,id.mylen");
        //6.用SQL API
        Table sqlResult = tableEnv.sqlQuery("select id,mylen(id) from " + table);

        //7.将表转换流输出
        tableEnv.toAppendStream(tableResult, Row.class).print("Table");
        tableEnv.toAppendStream(sqlResult,Row.class).print("SQL");
        //8.执行
        env.execute();

    }
    public static class MyLenth extends ScalarFunction{
        public int eval(String value){
            return value.length();
        }
    }
}
