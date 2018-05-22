package willem.weiyu.bigData.kafka

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

object ScalaFlinkKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val props = new Properties()
    props.setProperty("bootstrap.servers", "10.152.18.54:9092,10.152.18.55:9092,10.152.18.61:9092")
    props.setProperty("group.id", "flink-test")
    val consumer = new FlinkKafkaConsumer010[String]("fen_0310_test", new SimpleStringSchema(), props)
    consumer.setStartFromEarliest()

    val stream = env.addSource(consumer)

    val main1Stream = stream.filter(new FilterFunction[String] {
      override def filter(line: String): Boolean = {
        return "tm_app_main".equalsIgnoreCase(JSON.parseObject(line).getString("TABLENAME"))
      }
    }).flatMap((line:String,collector:Collector[(String,String,String)])=>{
      val jsonObj = JSON.parseObject(line)
      collector.collect((jsonObj.getString("APP_NO"), jsonObj.getString("PRODUCT_CD"), jsonObj.getString("CREATE_TIME")))
    })

    main1Stream.print()

    env.execute("scala flink-kafka")
  }
}
