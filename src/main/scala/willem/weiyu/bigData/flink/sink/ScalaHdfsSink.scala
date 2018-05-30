package willem.weiyu.bigData.flink.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object ScalaHdfsSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("10.152.18.36", 9999)
    //修改写入hdfs用户名
    System.setProperty("HADOOP_USER_NAME","gmjk")
    val hdfsSink = new BucketingSink[String]("hdfs://10.152.21.12:8020/user/weiyu/test/flink")
    hdfsSink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd"))
    hdfsSink.setWriter(new StringWriter[String]())
    hdfsSink.setUseTruncate(false)
    hdfsSink.setInactiveBucketCheckInterval(1)
    hdfsSink.setInactiveBucketThreshold(5)
    hdfsSink.setBatchSize(1024*1024*1024)

    text.addSink(hdfsSink)

    env.execute("scala hdfs sink")
  }
}
