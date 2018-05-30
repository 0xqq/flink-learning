package willem.weiyu.bigData.flink.source

import org.apache.flink.streaming.api.scala._

object ScalaSocketSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("10.152.18.36", 9999)

    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }

    counts.print()

    env.execute("scala window wordCount demo")
  }
}
