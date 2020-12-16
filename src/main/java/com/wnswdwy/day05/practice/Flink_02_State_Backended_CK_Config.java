package com.wnswdwy.day05.practice;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-12-15 18:27
 */
public class Flink_02_State_Backended_CK_Config {
    public static void main(String[] args) throws IOException {
        //1。 获执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 设置状态后端连接
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8082"));
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020"));


        //3. CK设置
        //3.1 开启CK
        env.enableCheckpointing(10000L);
        //3.2 设置两次CK开启时间间隔
        env.getCheckpointConfig().setCheckpointInterval(500L);
        //3.3 设置CK模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //3.4 设置同时有多少个CK任务
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(5);
        //3.5 设置CK超时时间
        env.getCheckpointConfig().setCheckpointTimeout(2);
        //3.6设置Ck重试次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //3.7 两次CK之间的间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        //3.8 如果存在更近的savePoint是否允许采用savePoint方式恢复
        env.getCheckpointConfig().setPreferCheckpointForRecovery(false);

        //4重启策略
        //4.1 固定延迟重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                Time.seconds(5)));
        //4.2失败重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3,Time.seconds(50),Time.seconds(5)));



    }
}
