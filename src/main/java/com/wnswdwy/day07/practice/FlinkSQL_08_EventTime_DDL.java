package com.wnswdwy.day07.practice;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yycstart
 * @create 2020-12-18 18:47
 */
public class FlinkSQL_08_EventTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //获取TableAPI的执行环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();


        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env,bsSettings);

        //2. 构建文件的DDL
        String sinkDDL = "create table dataTable(id varchar(20) not null," +
                "ts bigint," +
                "temp double," +
                "rt As To_TimeStamp(From_UnixTime(ts))," +
                "watermark for rt as rt - interval '1' second" +
                ")with(" +
                "'connector.type' = 'filesystem'," +
                "'connector.path' = 'sensor'," +
                "'format.type' = 'csv'" +
                ")";

        bsTableEnv.sqlUpdate(sinkDDL);

        //3.读取数据创建表
        Table dataTable = bsTableEnv.from("dataTable");
        //4.打印
        dataTable.printSchema();


    }
}
