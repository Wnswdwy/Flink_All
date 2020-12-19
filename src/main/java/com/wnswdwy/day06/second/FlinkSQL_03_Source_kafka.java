package com.wnswdwy.day06.second;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author yycstart
 * @create 2020-12-17 13:54
 */
public class FlinkSQL_03_Source_kafka {
    public static void main(String[] args) throws Exception {
        //1.获取连接环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("test")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "BigData20"))
          .withFormat(new Json())
          .withSchema(new Schema()
                  .field("id", DataTypes.STRING())
                  .field("ts",DataTypes.BIGINT())
                  .field("temp",DataTypes.DOUBLE()))
          .createTemporaryTable("kafka");

        Table table = tableEnv.from("kafka");
        Table tableResult = table.groupBy("id").select("id,temp.max");

        Table sqlResult = tableEnv.sqlQuery("select id,min(temp) from kafka group by id");

        tableEnv.toRetractStream(tableResult, Row.class).print("tablesResult");
        tableEnv.toRetractStream(sqlResult,Row.class).print("SQLResult");

        env.execute();


    }
}
