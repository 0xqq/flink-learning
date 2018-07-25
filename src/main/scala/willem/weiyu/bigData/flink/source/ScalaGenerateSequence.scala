package willem.weiyu.bigData.flink.source

import org.apache.flink.streaming.api.scala._

object ScalaGenerateSequence {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置缓存超时时间，调节吞吐及延迟
//    env.setBufferTimeout(10)

    val somIntegers:DataStream[Long] = env.generateSequence(0,10)
    val iteratedStream = somIntegers.iterate(
      iteration =>{
        val minusOne = iteration.map(v=> v-1)
        val strillGreaterThanZero = minusOne.filter(_ > 0)
        val lessThanZero = minusOne.filter(_ <= 0)
        (strillGreaterThanZero, lessThanZero)
      }
    )

    iteratedStream.print

    env.execute("scala generate sequence")
  }
}
