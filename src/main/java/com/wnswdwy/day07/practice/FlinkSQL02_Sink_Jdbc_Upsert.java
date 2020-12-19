package com.wnswdwy.day07.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.planner.plan.rules.logical.ReplaceMinusWithAntiJoinRule;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author yycstart
 * @create 2020-12-18 14:25
 */
public class FlinkSQL02_Sink_Jdbc_Upsert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.SQL
        tableEnv.createTemporaryView("sensor",sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,count(id)ct from sensor group by id");

        //定义MySQL DDL
        String sinkDDL  = "create table sqlOutputTable(id varchar(20) not null,ct bigint not null)"+
                "with('connector.type' = 'jdbc'," +
                    "'connector.url' = 'jdbc:mysql://hadoop102:3306/test'," +
                    "'connector.table' = 'sensor_ct'," +
                    "'connector.driver' = 'com.mysql.jdbc.Driver'," +
                    "'connector.username' = 'root'," +
                    "'connector.password' = '123456'," +
                    "'connector.write.flush.max-rows' = '1'" +
                        ")";
        tableEnv.sqlUpdate(sinkDDL);

        //5. 将数据写入MySQl
        sqlResult.insertInto("sqlOutputTable");

        //6.执行
        env.execute();

    }
}
