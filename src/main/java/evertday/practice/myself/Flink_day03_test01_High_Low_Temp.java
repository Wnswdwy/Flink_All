package evertday.practice.myself;

import bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Collections;

/**
 * @author yycstart
 * @create 2020-12-12 8:37
 */
public class Flink_day03_test01_High_Low_Temp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020:/flink"));
        //开启CK
        env.enableCheckpointing(5000L);
        //设置不自动删除CK
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> input = env.readTextFile("senor");

        SingleOutputStreamOperator<SensorReading> streamOperator = input.flatMap(new FlatMapFunction<String, SensorReading>() {
            @Override
            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
                String[] fields = value.split(",");
                out.collect(new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2])));
            }
        });

        SplitStream<SensorReading> splitResult = streamOperator.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() > 30 ? Collections.singletonList("high") : Collections.singletonList("low");

            }
        });

        DataStream<SensorReading> high = splitResult.select("high");
        DataStream<SensorReading> low = splitResult.select("low");
        high.print("high=>");
        low.print("low=>");

        env.execute();
    }
}
