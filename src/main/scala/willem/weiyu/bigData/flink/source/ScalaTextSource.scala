package willem.weiyu.bigData.flink.source

import org.apache.flink.streaming.api.scala._

object ScalaTextSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(10000)

    val lines = env.readTextFile("E://data.txt")

    lines.print()

    env.execute("scala text demo")
  }
}
