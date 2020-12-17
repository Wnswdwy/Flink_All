package com.wnswdwy.day06.practice;

import org.apache.calcite.schema.StreamableTable;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yycstart
 * @create 2020-12-16 15:37
 */
public class FlinkSQL_02_Env {
    public static void main(String[] args) {
        //老版的流式处理
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useOldPlanner() //老版的planner
                .inStreamingMode()//流处理模式
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


        //老版本批处理环境
        ExecutionEnvironment bathEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment.create(bathEnv);

        //新版本流式处理
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(env, bsSettings);


        //新版本批处理
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment bbTableEnv = TableEnvironment.create(bbSettings);
    }
}
