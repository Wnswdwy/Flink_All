package com.wnswdwy.day02.second;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author yycstart
 * @create 2020-12-11 22:16
 */
public class Flink20_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> socketReading = env.socketTextStream("hadoop102", 9999);

        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();



        socketReading.addSink(new RedisSink<>(jedisPoolConfig,new MyRedisSinkFunc()));

        env.execute();
    }
    public static class MyRedisSinkFunc implements RedisMapper<String>{

        /**
         * 指定写入类型，如果使用的是Hash或者ZSet，需要额外指定为外层Key
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"senor");
        }

        /**
         * 指定Redis中的Key的值(如果是Hash，指定的则是Field)
         * @param s
         * @return
         */
        @Override
        public String getKeyFromData(String s) {
            String[] spl = s.split(" ");
            return spl[0];
        }

        /**
         * 指定Redis中的Value
         * @param s
         * @return
         */
        @Override
        public String getValueFromData(String s) {
            String[] spl = s.split(",");
            return spl[2];
        }
    }
}
