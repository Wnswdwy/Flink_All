package com.wnswdwy.day02.second;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-11 22:05
 */
public class Flink19_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadooop102:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");
        socketTextStream.addSink(new FlinkKafkaProducer010<String>(
                "test",
                new SimpleStringSchema(),
                properties

        ));
        env.execute();
    }
}
