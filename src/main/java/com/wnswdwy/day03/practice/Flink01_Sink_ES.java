///*
//package com.wnswdwy.day03.practice;
//
//import bean.SensorReading;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.http.HttpHost;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//
//*
// * @author yycstart
// * @create 2020-12-12 10:22
//
//
//public class Flink01_Sink_ES {
//    public static void main(String[] args) {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//
//        DataStreamSource<String> input = env.readTextFile("senor");
//
//        SingleOutputStreamOperator<SensorReading> mapResult = input.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String value) throws Exception {
//                String[] fields = value.split(",");
//                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
//            }
//        });
//
//
//        //3. 写入ES
//
//        // es的httpHosts配置
//        ArrayList<HttpHost> httpHosts = new ArrayList<>();
//        httpHosts.add(new HttpHost("hadoop102", 9200));
//
//        input.addSink(new ElasticsearchSink<HttpHost>.Builder<>(httpHosts,new MyEsSinkFunction())
//
//    }
//    public static class MyEsSinkFunction implements ElasticsearchSinkFunction<SensorReading>{
//
//
//        @Override
//        public void process(SensorReading element, RuntimeContext ctx, RequestIndexer indexer) {
//            HashMap<String, Object> dataSource = new HashMap<>();
//            dataSource.put("id", element.getId());
//            dataSource.put("ts", element.getTs());
//            dataSource.put("temp", element.getTemp());
//
//            IndexRequest indexRequest = Requests.indexRequest()
//                    .index("sensor")
//                    .type("readingData")
//                    .source(dataSource);
//
//            indexer.add(indexRequest);
//
//        }
//    }
//
//}
//
//
//*/
