package com.wnswdwy.day07.practice;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yycstart
 * @create 2020-12-18 14:58
 */
public class FlinkSQL05_ProcessTime_DDL {
    public static void main(String[] args) {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2. 获取TableEnv的执行环境
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);

        //3. 构建文件的DDL
        String sinkDDL = "create table dataTable(" +
                "id varchar(20) not null," +
                "ts bigint," +
                "temp double," +
                "ps as proctime())" +
                "with(" +
                "'connector.type' = 'filesystem'," +
                "'connector.path'= 'sensor'," +
                "'format.type' = 'csv'" +
                ")";
        bsTableEnv.sqlUpdate(sinkDDL);

        //4. 读取数据创建表
        bsTableEnv.from("dataTable").printSchema();
    }
}
