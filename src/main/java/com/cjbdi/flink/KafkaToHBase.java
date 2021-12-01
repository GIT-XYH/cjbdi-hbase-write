package com.cjbdi.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * @Author: XYH
 * @Date: 2021/11/23 7:03 下午
 * @Description:
 */
public class KafkaToHBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "bd-01:9092");
        properties.setProperty("zookeeper.connect", "bd-01:2181");
        properties.setProperty("group.id", "consumer-group");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("tp1", new SimpleStringSchema(), properties);
        //从最早开始消费
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
        //stream.map();
        env.execute();
    }

}
