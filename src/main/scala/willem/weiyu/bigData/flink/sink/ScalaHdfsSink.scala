package willem.weiyu.bigData.flink.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

object ScalaHdfsSink {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("10.152.18.36", 9999)

    val hdfsSink = new BucketingSink[String]("hdfs://gmbdc-test/user/weiyu/test/flink")
    hdfsSink.setBucketer(new DateTimeBucketer[String]("yyyy-mm-dd"))
    //hdfsSink.setWriter(new SequenceFileWriter[IntWritable,Text]())
    hdfsSink.setBatchSize(1024*1024*1024)

    text.addSink(hdfsSink)

    env.execute("scala hdfs sink")
  }
}
