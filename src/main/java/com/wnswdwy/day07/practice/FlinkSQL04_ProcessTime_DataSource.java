package com.wnswdwy.day07.practice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author yycstart
 * @create 2020-12-18 14:49
 */
public class FlinkSQL04_ProcessTime_DataSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 构建文件连接器
        tableEnv.connect(new FileSystem().path("sensor"))
//                .withFormat(new OldCsv())
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                        .field("pt",DataTypes.TIMESTAMP(3)).proctime())
                .createTemporaryTable("sensorTable");

        //3. 读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");
        //4. 打印
        sensorTable.printSchema();
    }
}
