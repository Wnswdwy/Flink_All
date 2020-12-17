package com.wnswdwy.day06.practice;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.GroupedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author yycstart
 * @create 2020-12-16 16:12
 */
public class FlinkSQL_05_Sink_File {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据转换成JavaBean
        SingleOutputStreamOperator<SensorReading> socketMapFuncResult = env.socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, SensorReading>() {
                    @Override
                    public SensorReading map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                    }
                });

        //3.对流进行注册
        Table table = tableEnv.fromDataStream(socketMapFuncResult);

        //4. TableAPI
        Table tableResult = table.select("id,temp");

        //5. SQL
        tableEnv.createTemporaryView("sensor",socketMapFuncResult);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //6.创建文件连接器
        tableEnv.connect(new FileSystem().path("D:\\Z_Myself\\FlinkAll\\day06\\tableOut2"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("outTable1");
        tableEnv.connect(new FileSystem().path("D:\\Z_Myself\\FlinkAll\\day06\\sqlOut2"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("outTable2");

        //7. 将数据写入文件系统
        tableEnv.insertInto("outTable1",tableResult);
        tableEnv.insertInto("outTable2",sqlResult);

        //8.执行
        env.execute();

    }
}
