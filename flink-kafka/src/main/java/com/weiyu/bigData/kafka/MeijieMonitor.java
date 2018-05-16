package com.weiyu.bigData.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;

/**
 * @author weiyu
 * @description
 * @create 2018/5/16 10:43
 * @since 1.0.0
 */
public class MeijieMonitor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","10.152.18.54:9092,10.152.18.55:9092,10.152.18.61:9092");
        props.setProperty("group.id","flink-test");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("meijie_monitor_test", new SimpleStringSchema(),props);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        stream.print().setParallelism(1);

        env.execute("meijie_monitor");
    }
}
