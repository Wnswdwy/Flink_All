package com.wnswdwy.day06.practice;

import bean.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-16 19:32
 */
public class FlinkSQL_06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1。 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2. 从Kafka获取数据并转成JavaBean

        SingleOutputStreamOperator<SensorReading> socketMapResult = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });
        //3.对数据流进行注册
        Table table = tableEnv.fromDataStream(socketMapResult);
        //4. tableAPI
        Table tableResult = table.select("id,temp");
        //5. SQL
        tableEnv.createTemporaryView("sensor",socketMapResult);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from sensor");

        //6.创建Kafka连接器
        tableEnv.connect(new Kafka().topic("test").version("0.11").property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withFormat(new Csv())
//                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaOut");

        //7.将数据写入文件系统
        tableEnv.insertInto("kafkaOut", tableResult);
        tableEnv.insertInto("kafkaOut",sqlResult);

        //8. 执行
        env.execute();
    }
}
