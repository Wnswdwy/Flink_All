package com.wnswdwy.day07.teacher;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class FlinkSQL08_EventTime_DDL {

    public static void main(String[] args) {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //获取TableAPI执行环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        //2.构建文件的DDL
        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                //FROM_UNIXTIME(ts)将我们的秒转换成Linux时间，TO_TIMESTAMP将linux时间转换成时间戳ms
                " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
                //延迟一秒钟，相当于new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)）
                " watermark for rt as rt - interval '1' second" +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";
        bsTableEnv.sqlUpdate(sinkDDL);

        //3.读取数据创建表
        Table sensorTable = bsTableEnv.from("dataTable");

        //4.打印表的信息
        sensorTable.printSchema();

    }

}
