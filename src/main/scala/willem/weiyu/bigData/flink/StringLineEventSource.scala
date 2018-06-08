package willem.weiyu.bigData.flink

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import scala.util.Random

class StringLineEventSource extends RichParallelSourceFunction[String]{
  val LOG = LoggerFactory.getLogger(classOf[StringLineEventSource])
  @volatile private var running = true
  val channelSet = Seq("channel_1","channel_2","channel_3","channel_4")
  val behaviorTypes = Seq("INSTALL","OPEN","BROWSE","CLICK","PURCHASE","CLOSE","UNINSTALL")
  val rand = Random

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    var count = 0L

    while(running && count < Long.MaxValue){
      val channel = channelSet(rand.nextInt(channelSet.size))
      val event = generateEvent
      LOG.info("Event:"+event)
      val ts = event(0)
      val id = event(1)
      val behaviorType = event(2)
      ctx.collect(Seq(ts, channel, id, behaviorType).mkString("|"))
      count += 1
      TimeUnit.MILLISECONDS.sleep(1000L)
    }
  }

  private def generateEvent():Seq[Object] = {
    // simulate 10 seconds lateness
    val ts = Instant.ofEpochMilli(System.currentTimeMillis()).toEpochMilli
    val id = UUID.randomUUID().toString
    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
    //(ts, id, behaviorType)
    Seq(ts.toString, id, behaviorType)
  }

  override def cancel(): Unit = running = false
}
