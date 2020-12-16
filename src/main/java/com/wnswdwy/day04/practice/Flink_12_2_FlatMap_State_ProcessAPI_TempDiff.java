package com.wnswdwy.day04.practice;


import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-14 23:27
 */
public class Flink_12_2_FlatMap_State_ProcessAPI_TempDiff {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<SensorReading, Tuple> keyedStream = mapResult.keyBy("id");
        keyedStream.flatMap(new MyFlatMapTempDiff(10.0)).print();

        env.execute();
    }
    public static class MyFlatMapTempDiff extends RichFlatMapFunction<SensorReading, String> {

        private Double maxDiff;
        ValueState<Double> valueState;


        public MyFlatMapTempDiff(Double maxDiff) {
            this.maxDiff = maxDiff;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            valueState  = getRuntimeContext().getState(new ValueStateDescriptor<Double>("Temp-Diff", Double.class));

        }

        @Override
        public void flatMap(SensorReading value, Collector<String> out) throws Exception {

            //获取
            Double lastValue = valueState.value();
            value.getId();
            //更新
            valueState.update(value.getTemp());
            //判断
            if(! (valueState == null )&& (Math.abs(lastValue - value.getTemp()) > 10.0)){
                out.collect("两次温度差已经达到"+maxDiff+"度");
            }


        }
    }
}

