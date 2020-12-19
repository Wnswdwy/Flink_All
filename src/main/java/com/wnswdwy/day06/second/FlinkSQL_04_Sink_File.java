package com.wnswdwy.day06.second;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;


/**
 * @author yycstart
 * @create 2020-12-17 14:55
 */
public class FlinkSQL_04_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        SingleOutputStreamOperator<SensorReading> socketMapResult = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        Table table = tableEnv.fromDataStream(socketMapResult);

        Table tableResult = table.select("id,temp");

        tableEnv.createTemporaryView("sensor",socketMapResult);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //
        tableEnv.connect(new FileSystem().path("tableOutPut"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("tableOut");
        tableEnv.connect(new FileSystem().path("SQLOutPut"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("SQLOut");

        tableEnv.insertInto("tableOut",tableResult);
        tableEnv.insertInto("SQLOut",sqlResult);

        env.execute();
    }
}
