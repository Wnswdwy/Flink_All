package com.wnswdwy.day06.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yycstart
 * @create 2020-12-16 11:45
 */
public class FlinkSQL01_Test {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取TableAPI执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2. 读取端口数据，将每一行数据转换成JavaBean
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        //3. 对流进行注册
        tableEnv.createTemporaryView("sensor",mapResult);

        //4. SQL方式实现查询
        Table sqlResult = tableEnv.sqlQuery("select id,count(id)from sensor group by id");
        //5.TableAPI的方式
        Table table = tableEnv.fromDataStream(mapResult);
        Table selectResult = table.groupBy("id").select("id,id.count");

        //6.将表转换成流进行输出
        tableEnv.toAppendStream(sqlResult, Row.class).print("SQL");
        tableEnv.toAppendStream(selectResult,Row.class).print("TableAPI");


        //7.启动
        env.execute();

    }
}
