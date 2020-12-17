package com.wnswdwy.day06.practice;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author yycstart
 * @create 2020-12-16 20:45
 */
public class FlinkSQL_07_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2. 从socket读取数据
        SingleOutputStreamOperator<SensorReading> sockMapResult = env.socketTextStream("hadoop102", 9999).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        //3. 对流进行注册
        Table table = tableEnv.fromDataStream(sockMapResult);
        //4.TableAPI
        Table tableResult = table.select("id,temp");
        //5.SQL
        tableEnv.createTemporaryView("sensor",sockMapResult);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");
        //6.创建ES连接器
        tableEnv.connect(new Elasticsearch()
                            .version("6")
                            .host("hadoop102", 9200, "http")
                            .index("sensor2")
                            .documentType("_doc")
                            .bulkFlushMaxActions(1))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .inAppendMode()
                .createTemporaryTable("Es");

        //7. 将数据写入文件系统
        tableEnv.insertInto("Es",tableResult);
        tableEnv.insertInto("Es",sqlResult);

        //8.执行
        env.execute();
    }
}
