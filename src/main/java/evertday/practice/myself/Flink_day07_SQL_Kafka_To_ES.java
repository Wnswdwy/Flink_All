package evertday.practice.myself;

import bean.KafkaCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-18 8:35
 *
 *
 * 输入数据如下:
 * hello,atguigu,hello
 * hello,spark
 * hello,flink
 *
 * 使用FlinkSQL实现从Kafka读取数据计算WordCount并将数据写入ES中
 */
public class Flink_day07_SQL_Kafka_To_ES {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.从kafka读取数据，并转换成JavaBean
        //从Kafka中读取数据
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "BigData20");
        properties.setProperty("key.serialization", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("auto.offset.reset", "latest");


        SingleOutputStreamOperator<KafkaCount> kafkaToOne = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties
        )).flatMap(new MyFlatMapFunc());




    //3.对流进行注册
    Table table = tableEnv.fromDataStream(kafkaToOne);

    //4.TableAPI
    Table tableResult = table.groupBy("id").select("id,count(id) sumCount");

    //5.SQL
        tableEnv.createTemporaryView("sensor", kafkaToOne);
    Table sqlResult = tableEnv.sqlQuery("select id,count(id) sumCount from sensor group by id");

    //6.创建ES连接器
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102", 9200, "http")
                .index("sensor10")
                .documentType("_doc")
                .bulkFlushMaxActions(1))
            .withFormat(new Json())
            .withSchema(new Schema()
                        .field("id",DataTypes.STRING())
                        .field("sumCount", DataTypes.INT()))
            .inAppendMode()
                .createTemporaryTable("Es");

    //7.将数据写入文件系统
        tableEnv.insertInto("Es", tableResult);
        tableEnv.insertInto("Es", sqlResult);

    //8.执行
        env.execute();


    }
    public static class MyFlatMapFunc implements FlatMapFunction<String,KafkaCount>{

        @Override
        public void flatMap(String value, Collector<KafkaCount> out) throws Exception {
            String[] words = value.split(",");
            for (String word : words) {
                out.collect(new KafkaCount(word,1));
            }
        }
    }
}