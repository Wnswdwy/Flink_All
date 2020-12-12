package com.wnswdwy.day02.second;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-11 18:52
 */
public class Flink06_Source_Kafka {
    public static void main(String[] args) throws Exception {
            //获取环境连接
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            //读取Kafka数据
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
            properties.put(ConsumerConfig.GROUP_ID_CONFIG,"BigData2020");
            properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty("auto.offset.reset","latest");

            DataStreamSource<String> senorDS = env.addSource(new FlinkKafkaConsumer011<String>(
                    "test",
                    new SimpleStringSchema(),
                    properties
            ));

            //3. 打印
            senorDS.print();

            //4.启动
            env.execute();
        }
}
