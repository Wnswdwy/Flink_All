package com.wnswdwy.day07.practice;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author yycstart
 * @create 2020-12-18 18:20
 */
public class FlinkSQL_07_EventTime_DataSource {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 创建文件连接器
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        //自定义名称
                        .field("rts",DataTypes.BIGINT()).rowtime(new Rowtime()
                                .timestampsFromField("ts") //从字段中提取时间戳
                                .watermarksPeriodicBounded(1000L)) //watermark延迟1s
                        .field("temp",DataTypes.DOUBLE())
                ).createTemporaryTable("sensorTable");

        //3.读取数据创建表
        Table sensorTable = tableEnv.from("sensorTable");
        //4.打印表的数据
        sensorTable.printSchema();


    }
}
