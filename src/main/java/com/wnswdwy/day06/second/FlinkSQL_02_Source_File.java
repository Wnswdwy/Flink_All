package com.wnswdwy.day06.second;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-17 11:48
 */
public class FlinkSQL_02_Source_File {
    public static void main(String[] args) {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.构建文件的连接器、
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                ).createTemporaryTable("senorTemporary");
        //3.读取数据创建表
        Table sourceTable = tableEnv.from("senorTemporary");
        //4.sql
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sourceTable");
        //TableAPI
        Table tableResult = sourceTable.select("id,temp.max").where("id='sensor_1'");

        //6.转换为流输出
        tableEnv.toRetractStream(tableResult,Row.class).print("tableResult");
        tableEnv.toRetractStream(sqlResult,Row.class).print("sqlResult");
    }
}
