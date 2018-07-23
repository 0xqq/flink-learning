package willem.weiyu.bigData.flink.source

import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition
import org.apache.flink.util.Collector

object ScalaKafkaSource {
  private val sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "10.152.18.54:9092,10.152.18.55:9092,10.152.18.61:9092")
    props.setProperty("group.id", "flink-test")
    val consumer = new FlinkKafkaConsumer010[String]("fen_0310_test", new SimpleStringSchema(), props)
    //默认偏移量获取
//    consumer.setStartFromGroupOffsets()
    //从最早开始消费
//    consumer.setStartFromEarliest()
    //指定消费位置
//    val specificStartOffsets = new util.HashMap[KafkaTopicPartition, Long]()
//    specificStartOffsets.put(new KafkaTopicPartition("fen_0310_test",0), 23L)
//    consumer.setStartFromSpecificOffsets(specificStartOffsets)

    val stream = env.addSource(consumer)

    val main1Stream = stream.filter(new FilterFunction[String] {
      override def filter(line: String): Boolean = {
        return "tm_app_main".equalsIgnoreCase(JSON.parseObject(line).getString("TABLENAME"))
      }
    }).flatMap((line: String, collector: Collector[(String, String, String)]) => {
      val jsonObj = JSON.parseObject(line)
      collector.collect((jsonObj.getString("APP_NO"), jsonObj.getString("PRODUCT_CD"), jsonObj.getString("CREATE_TIME")))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, String, String)](Time.milliseconds(10000)) {
      override def extractTimestamp(element: (String, String, String)): Long = {
        return sdf.parse(element._3).getTime
      }
    })

    //    main1Stream.print()
    main1Stream.writeAsText("hdfs://gmbdc-test/user/weiyu/test/flink")

    env.execute("scala flink-kafka")
  }
}
