package willem.weiyu.bigData.flink.time

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import willem.weiyu.bigData.flink.StringLineEventSource

object ScalaEventTime {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val line = env.addSource(new StringLineEventSource)
    val inputMap = line.map(word=>{
      val arr = word.split("\\|")
      (arr(0).toLong, arr(1), arr(2), arr(3))
    })
    /*val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String, String)] {

      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 5000L//最大允许的乱序时间是5s

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: (Long, String, String, String), lastTimestamp: Long): Long = {
        val timestamp = t._1
        currentMaxTimestamp = Math.max(timestamp, lastTimestamp)
        println("timestamp:" + t._1 + "|" +"currentMaxTimestamp:" +","+  currentMaxTimestamp +"|"+t.toString)
        timestamp
      }
    })*/

    val watermark = inputMap.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, String)](Time.milliseconds(5000L)) {
      override def extractTimestamp(element: (Long, String, String, String)): Long = {
        println("======timestamp:" + element._1 +"|"+element.toString)
        element._1
      }
    })

    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(5)))
        .apply(new MyWindowFunction)

    window.print()

    env.execute()
  }

  class MyWindowFunction extends WindowFunction[(Long, String, String, String),(Long, String,String,String),Long,TimeWindow]{

    override def apply(key: Long, window: TimeWindow, input: Iterable[(Long, String, String, String)], out: Collector[(Long, String, String, String)]): Unit = {
      val list = input.toList.sortBy(_._1)
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,format.format(list.head._1),format.format(list.last._1),"["+format.format(window.getStart)+","+format.format(window.getEnd)+")")
    }
  }
}
