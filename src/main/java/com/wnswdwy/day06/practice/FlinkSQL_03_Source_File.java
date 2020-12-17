package com.wnswdwy.day06.practice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-16 15:53
 */
public class FlinkSQL_03_Source_File {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 构建文件的连接器
        tableEnv.connect(new FileSystem().path("senor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                ).createTemporaryTable("sensorTable");

        //3. 读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");
        //4.TableAPIz
        Table tableResult = sensorTable.select("id,temp").where("id='sensor_1'");
        //5.SQL方式
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensorTable where id = 'sensor_1'");

        //6.转换成流输出
        tableEnv.toRetractStream(tableResult, Row.class).print("Table");
        tableEnv.toRetractStream(sqlResult, Row.class).print("SQL");

        //7.执行
        env.execute();
    }
}
