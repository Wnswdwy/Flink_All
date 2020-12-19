package com.wnswdwy.day06.second;

import bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author yycstart
 * @create 2020-12-17 15:23
 */
public class FlinkSQL_06_Sink_ES {
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

        tableEnv.connect(new Elasticsearch().bulkFlushMaxActions(1)
                        .index("senor5")
                        .host("hadoop102",9200,"http")
                        .version("6")
                        .documentType("_doc"))
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("temp",DataTypes.DOUBLE()))
                .inAppendMode()
                .createTemporaryTable("ES");



        tableEnv.insertInto("ES",tableResult);
        tableEnv.insertInto("ES",sqlResult);

        env.execute();

    }
}
