package com.wnswdwy.day03.practice;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author yycstart
 * @create 2020-12-12 21:17
 */
public class Flink02_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 读取Sock数据
        DataStreamSource<String> socketResult = env.socketTextStream("hadoop102", 9999);


        //3. 读取JDBC
        DataStreamSink<String> result = socketResult.addSink(new MyJDBC());

        //4. 执行
        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<String>{
        //声明JDBC连接
        private Connection connection;
        //声明预编译语句
        private PreparedStatement preparedStatement;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //声明连接
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/student","root","123456");
            //声明预编译语句
            preparedStatement = connection.prepareStatement("insert into senor(id,temp) values(?,?) on duplicate Key undate temp =?;");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //切割数据
            String[] fields = value.split(",");
            //给预编译语句赋值
            preparedStatement.setString(1, fields[0]);
            preparedStatement.setString(2, fields[1]);
            preparedStatement.setString(3, fields[2]);

            //执行
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
