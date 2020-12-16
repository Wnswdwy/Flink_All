package com.wnswdwy.day05.practice;

import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yycstart
 * @create 2020-12-15 18:56
 */
public class Flink_01_01_State_OnTimer_TempDiff {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);


        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });


        KeyedStream<SensorReading, String> keyedStream = mapResult.keyBy(SensorReading::getId);

        SingleOutputStreamOperator<String> process = keyedStream.process(new MyKeyedProcessFunc(10));

        process.print();

        env.execute();
    }
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String,SensorReading,String>{
        ValueState<Double> tmpState;

        ValueState<Long> tsState;

        private Integer interval;

        public MyKeyedProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tmpState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取上一次温度
            Double lastTemp = tmpState.value();
            //获取当前温度
            Double curTemp = value.getTemp();
            //获取当前时间状态
            Long ts = tsState.value();
            //更新温度状态
            tmpState.update(curTemp);
            if (lastTemp < curTemp && ts == null){
                //获当前时间 + 报警时间10s
                Long curTs = ctx.timerService().currentProcessingTime() + interval*1000L;
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(curTs);
                //更新时间状态
                tsState.update(curTs);
            }else if(lastTemp > curTemp && ts != null){
                ctx.timerService().deleteProcessingTimeTimer(ts);
                tsState.clear();
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + " 没有降温了！！");
            tsState.clear();
        }
    }
}
