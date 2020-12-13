
package com.wnswdwy.day03.practice;



/*
 * @author yycstart
 * @create 2020-12-12 10:22
*/

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink01_Sink_ES {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 读取端口数据
        DataStreamSource<String> sockResult = env.socketTextStream("hadoop102", 9999);


        //3. 写入ES
        //3.2 准备集群连接参数
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9092));

        //3.1 准备创建ES Sink Builder
        ElasticsearchSink.Builder<String> stringBuilder = new ElasticsearchSink.Builder<>(httpHosts, new MyEsSinkFunc());


        //3.3 设置刷写条数
        stringBuilder.setBulkFlushInterval(1);

        //3.4 创建EsSink
        ElasticsearchSink<String> elasticsearchSink = stringBuilder.build();

        sockResult.addSink(elasticsearchSink);
        sockResult.print("result");

        //4. 执行
        env.execute();

    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<String>{

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {

            System.out.println(element);

            //切割
            String[] fields = element.split(",");

            //创建Map存放数据
            HashMap<String, String> hashMap = new HashMap<>();
            hashMap.put("id",fields[0]);
            hashMap.put("ts",fields[1]);
            hashMap.put("temp",fields[2]);
            
            
            //创建IndexRequest
            IndexRequest indexRequest = Requests.indexRequest().index("senor").type("_doc").id(fields[0]).source(hashMap);
            
            //写入ES
            indexer.add(indexRequest);


        }
    }
}