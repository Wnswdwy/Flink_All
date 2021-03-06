package evertday.practice.myself;
import bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.RichProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/*
 * @author yycstart
 * @create 2020-12-16 8:34
 *
 *
 * 2.从Kafka读取传感器数据,统计每个传感器发送温度的次数存入MySQL(a表),
 * 如果某个传感器温度连续10秒不下降,则输出报警信息到侧输出流并存入MySQL(b表).
 *
 */


public class Flink_day06_Test02_KafkaToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"BigData20");

        properties.setProperty("key.serialization","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> kafkaSource = env.addSource(new FlinkKafkaConsumer011<String>(
                "test",
                new SimpleStringSchema(),
                properties
        ));

        SingleOutputStreamOperator<SensorReading> mapSensorResult = kafkaSource.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        KeyedStream<SensorReading, String> keyedStream = mapSensorResult.keyBy(SensorReading::getId);

        //s4.使用ProcessAPI实现10秒温度没有下降则报警逻辑
        SingleOutputStreamOperator<String> process = keyedStream.process(new MyKeyedProcessFunc(10));

        DataStream<String> outPut = process.getSideOutput(new OutputTag<String>("Error"){});
        //5. 将主流和侧输出写入MySQL
        process.addSink(new MyJdbcSink("insert into mainOut(id,ts) values(?,?) on ON DUPLICATE KEY UPDATE ct=?;",3));
        outPut.addSink(new MyJdbcSink("insert into error(err)values(?)",1 ));

        //6.打印测试
        process.print("Main");
        outPut.print("OutPut");

        env.execute();
    }
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<String,SensorReading,String>{
        ValueState<Double> tempState;
        ValueState<Long> tsState;
        ValueState<Integer> countState;
        private Integer diffTemp;


        public MyKeyedProcessFunc(Integer diffTemp) {
            this.diffTemp = diffTemp;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            tempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("temp-state", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count-state", Integer.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {


            Double lastTemp = tempState.value();
            Long ts = tsState.value();
            Double curTemp = value.getTemp();
            Integer count = countState.value();
            tempState.update(curTemp);
            countState.update(count+1);

            if(lastTemp < curTemp && ts == null ){
                long curTs = ctx.timerService().currentProcessingTime() + diffTemp *1000L;
                ctx.timerService().registerProcessingTimeTimer(curTs);
                tsState.update(curTs);
            }else if(lastTemp > curTemp && ts != null){
                ctx.timerService().deleteProcessingTimeTimer(ts);
                tsState.clear();
            }
            out.collect(value.getId()+","+countState.value());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ctx.output(new OutputTag<String>("Error"){},ctx.getCurrentKey()+"在"+timestamp+"温度已经连续"+diffTemp+"没有下降了");
            tsState.clear();
        }
    }
    public static class MyJdbcSink extends RichSinkFunction<String> {
        private String sql;
        private int number;

        public MyJdbcSink(String sql, int number) {
            this.sql = sql;
            this.number = number;
        }

        Connection connection;
        PreparedStatement preparedStatement;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/jdbc", "root", "123456");
            preparedStatement  = connection.prepareStatement(sql);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //将传入的数据进行处理
            String[] fields = value.split(",");
            //给预编译赋值
            for (Integer i = 0; i < number; i++) {

                if(i == 2){
                    preparedStatement.setObject(i + 1, fields[i-1]);
                }else {
                    preparedStatement.setObject(i + 1, fields[i]);
                }
            }
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
