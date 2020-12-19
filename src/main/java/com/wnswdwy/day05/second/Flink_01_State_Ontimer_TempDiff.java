package com.wnswdwy.day05.second;

import bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-17 15:43
 */
public class Flink_01_State_Ontimer_TempDiff {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<SensorReading> socketMapRedult = env.socketTextStream("hadoop102", 9999)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        KeyedStream<SensorReading, String> keyedStream = socketMapRedult.keyBy(SensorReading::getId);

        keyedStream.process(new MyProcess(10)).print();

        env.execute();
    }
    public static class MyProcess extends KeyedProcessFunction<String,SensorReading,String> {
        private int diffTemp;

        public MyProcess(int diffTemp) {
            this.diffTemp = diffTemp;
        }
        ValueState<Double> valueState;
        ValueState<Long> tsState;
        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            Double lastTemp = valueState.value();
            Double curTemp = value.getTemp();
            Long ts = tsState.value() ;
            valueState.update(curTemp);
            if(lastTemp < curTemp && ts == null){
                long curTs = ctx.timerService().currentProcessingTime();
                ctx.timerService().registerProcessingTimeTimer(curTs) ;
                tsState.update(curTs);
            }else if(lastTemp > curTemp && ts != null){
                ctx.timerService().deleteProcessingTimeTimer(ts);
                tsState.clear();
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+"没有降温了！" );
            tsState.clear();
        }
    }
}
