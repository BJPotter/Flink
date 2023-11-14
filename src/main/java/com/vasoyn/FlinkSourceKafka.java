package com.vasoyn;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * flink 读取kafka数据
 */
public class FlinkSourceKafka {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从kafka连接配置
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"180.76.187.95:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group-id");

        //3.kafka消费者配置
        FlinkKafkaConsumer<Tuple2<String,Integer>> kafkaConsumer = new FlinkKafkaConsumer(
                "liu", // 逗号分隔的多个主题
                new SimpleStringSchema(),
                properties); //


        //4.按照DataStream<Tuple2<String,Integer>>，读取topic中的数据
        DataStream<Tuple2<String,Integer>> dataStream = env.addSource(kafkaConsumer);

        System.out.println(dataStream.print());

        //执行作业
        env.execute("Flink Kafka Source Example");


    }

}
