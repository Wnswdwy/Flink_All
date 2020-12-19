package com.wnswdwy.day07.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author yycstart
 * @create 2020-12-18 14:05
 */
public class FlinkSQL01_Sink_Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 读取数据并转换成JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });
        //3. 对数据流进行注册
        Table table = tableEnv.fromDataStream(sensorDS);
        //4. TableAPI
        Table tableResult = table.select("id,temp");
        //5. 定义MySQL的DDL
        String sinkDDL = "create table jdbcOutPutTable(id varchar(20) not null,temp double not null)"+
        "with("+
                "'connector.type'='jdbc',"+
                "'connector.url' = 'jdbc:mysql://hadoop102:3306/test'," +
                "'connector.table' = 'sensor_temp',"+
                "'connector.driver' = 'com.mysql.jdbc.Driver',"+
                "'connector.username' = 'root',"+
                "'connector.password' = '123456'"+
                "'connector.write.flush.max-rows' = '1')";
        tableEnv.sqlUpdate(sinkDDL);

        //6.将数据写入MySQL
        tableResult.insertInto("jdbcOutPutTable");

        //7.执行
        env.execute();


    }
}
