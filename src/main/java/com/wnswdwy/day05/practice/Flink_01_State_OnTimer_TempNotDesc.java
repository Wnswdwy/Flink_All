package com.wnswdwy.day05.practice;


import bean.SensorReading;
import lombok.val;
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
 * @create 2020-12-15 15:13
 *
 * /4.使用ProcessAPI实现10秒温度没有下降则报警逻辑
 */
public class Flink_01_State_OnTimer_TempNotDesc {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2. 从socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3 将数据转化成JavaBean
        SingleOutputStreamOperator<SensorReading> mapResult = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4.分组
        KeyedStream<SensorReading, String> keyedStream = mapResult.keyBy(SensorReading::getId);

        //5.使用ProcessAPI实现10秒温度没有下降则报警逻辑

        keyedStream.process(new MyKeyedProcessFunc(10)).print();

        //执行
        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String, SensorReading, String> {

        //定义属性,时间间隔
        private Integer interval;

        //声明状态用于存储每一次的温度值
        private ValueState<Double> tempState;

        //声明状态用于存放定时器时间
        private ValueState<Long> tsState;

        public MyKeyedProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class, Double.MIN_VALUE));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }



        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取上一次温度值
            Double lastTemp = tempState.value();
            //获取当前温度
            Double curTemp = value.getTemp();
            //获取当前时间状态
            Long ts = ctx.timerService().currentProcessingTime();
            //更新温度状态
            tempState.update(curTemp);

            if(lastTemp < curTemp && ts == null){
                //获取当前时间
                long curTime = ctx.timerService().currentProcessingTime() + interval * 1000L;
                //注册
                ctx.timerService().registerProcessingTimeTimer(curTime);
                //更新时间状态
                tsState.update(curTime);
            }else if(lastTemp  > curTemp && ts != null){
                //删除定时器
                ctx.timerService().deleteProcessingTimeTimer(ts);
                //时间状态清空
                tsState.clear();
            }

        }
        //定时器触发任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect("没有连续降温"+ctx.getCurrentKey());
            tsState.clear();
        }

    }

}
