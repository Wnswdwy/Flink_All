package com.wnswdwy.day02.practice;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-11 11:12
 */
public class Flink02_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 从kafka中读取数据
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"BigData2020");
        properties.setProperty("key.deserialization","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> senorDS = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties
        ));

        //3. 打印
        senorDS.print();

        //4. 启动
        env.execute();

    }
}
