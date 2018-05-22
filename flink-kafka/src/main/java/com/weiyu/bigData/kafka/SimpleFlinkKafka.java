package com.weiyu.bigData.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author weiyu
 * @description
 * @create 2018/5/15 13:41
 * @since 1.0.0
 */
public class SimpleFlinkKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000,CheckpointingMode.EXACTLY_ONCE);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","10.152.18.54:9092,10.152.18.55:9092,10.152.18.61:9092");
        props.setProperty("group.id","flink-test");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("fen_0310_test", new SimpleStringSchema(),props);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        /*DataStream<JSONObject> historyStream = stream.filter((FilterFunction<String>) s -> {
            JSONObject jsonObj = JSONObject.parseObject(s);
            return "tm_app_history".equalsIgnoreCase(jsonObj.getString("TABLENAME"));
        }).flatMap(new FlatMapFunction<String, JSONObject>() {
            public void flatMap(String line, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(line);
                *//*JSONObject result = new JSONObject();
                result.put("ID",jsonObj.getString("ID"));
                result.put("APP_NO",jsonObj.getString("APP_NO"));
                result.put("NAME",jsonObj.getString("NAME"));
                result.put("CREATE_TIME",jsonObj.getString("CREATE_TIME"));*//*
                collector.collect(jsonObj);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<JSONObject>(Time.minutes(1)) {
            @Override
            public long extractTimestamp(JSONObject element) {
                String date = element.getString("CREATE_TIME");
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                try {
                    return sdf.parse(date).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        });*/

        DataStream<Tuple3<String,String,String>> main1Stream = stream.filter((FilterFunction<String>) s -> {
            JSONObject jsonObj = JSONObject.parseObject(s);
            return "tm_app_main".equalsIgnoreCase(jsonObj.getString("TABLENAME"));
        }).flatMap(new FlatMapFunction<String, Tuple3<String,String,String>>() {
            public void flatMap(String line, Collector<Tuple3<String,String,String>> collector) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(line);
                Tuple3 result = new Tuple3();
                result.setFields(jsonObj.getString("APP_NO"),jsonObj.getString("PRODUCT_CD"),jsonObj.getString("CREATE_TIME"));
                collector.collect(result);

            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, String, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, String, String> element) {
                return 0;
            }
        });

        DataStream<Tuple2<String,String>> main2Stream = stream.filter((FilterFunction<String>) s -> {
            JSONObject jsonObj = JSONObject.parseObject(s);
            return "tm_app_main".equalsIgnoreCase(jsonObj.getString("TABLENAME"));
        }).flatMap(new FlatMapFunction<String, Tuple2<String,String>>() {
            public void flatMap(String line, Collector<Tuple2<String,String>> collector) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(line);
                Tuple2 result = new Tuple2();
                result.setFields(jsonObj.getString("APP_NO"),jsonObj.get("NAME"));
                collector.collect(result);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, String>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, String> element) {
                return 0;
            }
        });

        DataStream<Tuple5<String, String, String, String, String>> joinMain = main1Stream.join(main2Stream).where(new KeySelector<Tuple3<String,String,String>, String>() {
            @Override
            public String getKey(Tuple3<String, String, String> tuple3) throws Exception {
                return tuple3.f0;
            }
        }).equalTo(new KeySelector<Tuple2<String, String>, String>() {
            @Override
            public String getKey(Tuple2<String, String> tuple2) throws Exception {
                return tuple2.f0;
            }
        }).window(GlobalWindows.create())
                .apply(new JoinFunction<Tuple3<String, String, String>, Tuple2<String, String>, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> join(Tuple3<String, String, String> tuple3, Tuple2<String, String> tuple2) throws Exception {
                        return new Tuple5(tuple3.f0,tuple3.f1,tuple3.f2,tuple2.f0,tuple2.f1);
                    }
                });

        joinMain.print().setParallelism(1);

        env.execute("flink-kafka demo");
    }
}
