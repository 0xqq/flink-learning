package willem.weiyu.bigData.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("10.152.18.36", 9999)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("scala window wordCount demo")
  }
}
