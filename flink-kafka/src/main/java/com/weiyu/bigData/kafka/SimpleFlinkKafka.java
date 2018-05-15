package com.weiyu.bigData.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author weiyu
 * @description
 * @create 2018/5/15 13:41
 * @since 1.0.0
 */
public class SimpleFlinkKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","10.152.18.54:9092,10.152.18.55:9092,10.152.18.61:9092");
        props.setProperty("group.id","flink-test");
        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("fen_0310_test", new SimpleStringSchema(),props);
        consumer.setStartFromEarliest();
        DataStream<String> stream = env.addSource(consumer);

        DataStream<JSONObject> historyStream = stream.filter((FilterFunction<String>) s -> {
            JSONObject jsonObj = JSONObject.parseObject(s);
            return "tm_app_history".equalsIgnoreCase(jsonObj.getString("TABLENAME"));
        }).flatMap(new FlatMapFunction<String, JSONObject>() {
            public void flatMap(String line, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(line);
                /*JSONObject result = new JSONObject();
                result.put("ID",jsonObj.getString("ID"));
                result.put("APP_NO",jsonObj.getString("APP_NO"));
                result.put("NAME",jsonObj.getString("NAME"));
                result.put("CREATE_TIME",jsonObj.getString("CREATE_TIME"));*/
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
        });

        /*DataStream<JSONObject> mainStream = stream.filter((FilterFunction<String>) s -> {
            JSONObject jsonObj = JSONObject.parseObject(s);
            return "tm_app_main".equalsIgnoreCase(jsonObj.getString("TABLENAME"));
        }).flatMap(new FlatMapFunction<String, JSONObject>() {
            public void flatMap(String line, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(line);
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

        /*DataStream<JSONObject> joinDataStream = mainStream.join(infoStream).where(new KeySelector<JSONObject, String>() {

            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("APP_NO");
            }
        }).equalTo(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getString("APP_NO");
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10L))).apply((JoinFunction<JSONObject, JSONObject, JSONObject>) (jsonObject, jsonObject2) -> {
            JSONObject result = new JSONObject();
            result.put("obj1",jsonObject);
            result.put("obj2",jsonObject2);
            return result;
        });*/

        historyStream.print().setParallelism(1);

        env.execute("flink-kafka demo");
    }
}
