package willem.weiyu.bigData.flink.source

import org.apache.flink.streaming.api.scala._
import willem.weiyu.bigData.flink.StringLineEventSource

object ScalaCustomSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new StringLineEventSource)
    text.print

    env.execute("scala custom")
  }
}
