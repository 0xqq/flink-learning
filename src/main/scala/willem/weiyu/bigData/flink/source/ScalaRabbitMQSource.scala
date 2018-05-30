package willem.weiyu.bigData.flink.source

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.rabbitmq.RMQSource
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig

object ScalaRabbitMQSource {
  val topicName = "gome-dq-queue-test"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    val connectionConfig = new RMQConnectionConfig.Builder()
      .setHost("http://10.152.18.36")
      .setPort(5672)
      .setVirtualHost("dq")
      .setUserName("guest")
      .setPassword("guest")
      .build()

    val stream = env.addSource(new RMQSource[String](connectionConfig,topicName,false,new SimpleStringSchema())).setParallelism(1)

    stream.print

    env.execute("rabbitmq demo")
  }
}
