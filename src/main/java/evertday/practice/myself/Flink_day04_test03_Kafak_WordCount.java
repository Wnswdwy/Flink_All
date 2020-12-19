package evertday.practice.myself;



import com.wnswdwy.day03.practice.Flink02_Sink_JDBC;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author yycstart
 * @create 2020-12-14 8:45
 *
 *
 * 3.读取Kafka主题的数据计算WordCount并存入MySQL.
 *
 */
public class Flink_day04_test03_Kafak_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //从Kafka中读取数据
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"BigData20");
        properties.setProperty("key.serialization","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("auto.offset.reset","latest");


        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties
        ));

        DataStreamSink<String> addSink = kafkaSource.addSink(new Flink02_Sink_JDBC.MyJDBC());

        env.execute();


    }

    public static class MyJDBC extends RichSinkFunction<String>{

        //声明JDBC连接
        Connection connection;
        //声明预编译语句
        PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
           connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            preparedStatement = connection.prepareStatement("insert into te (id,temp,ts) values(?,?,?) on duplicate Key update temp =?;");
        }


        @Override
        public void invoke(String value, Context context) throws Exception {
            String[] fields = value.split(",");
            preparedStatement.setString(1, fields[0]);
            preparedStatement.setString(2, fields[1]);
            preparedStatement.setString(3, fields[2]);
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            super.close();
            preparedStatement.close();
            connection.close();
        }
    }
}
