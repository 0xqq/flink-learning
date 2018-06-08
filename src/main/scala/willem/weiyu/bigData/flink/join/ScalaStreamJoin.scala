package willem.weiyu.bigData.flink.join

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import willem.weiyu.bigData.flink.StringLineEventSource

object ScalaStreamJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val line = env.addSource(new StringLineEventSource)
    val inputMap = line.map(word=>{
      val arr = word.split("\\|")
      (arr(0).toLong, arr(1), arr(2), arr(3))
    })

    val watermark = inputMap.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, String)](Time.milliseconds(5000L)) {
      override def extractTimestamp(element: (Long, String, String, String)): Long = {
        println("watermark======timestamp:" + element._1 +"|"+element.toString)
        element._1
      }
    })

    val watermark2 = inputMap.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String, String, String)](Time.milliseconds(5000L)) {
      override def extractTimestamp(element: (Long, String, String, String)): Long = {
        println("watermark2======timestamp:" + element._1 +"|"+element.toString)
        element._1
      }
    })

    val joinStream = watermark.join(watermark2).where(_._3).equalTo(_._3).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply{
      (t1 : (Long, String, String, String), t2 : (Long, String, String, String), out : Collector[(Long,String,String,String,Long,String,String,String)]) =>
        out.collect(t1._1,t1._2,t1._3,t1._4,t2._1,t2._2,t2._3,t2._4);
    };

    joinStream.print

    env.execute()
  }
}
