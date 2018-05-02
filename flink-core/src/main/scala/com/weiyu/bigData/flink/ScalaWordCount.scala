package com.weiyu.bigData.flink

import java.io.File

import org.apache.flink.api.scala._

object ScalaWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("E:"+File.separator+"test.txt")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()

    env.execute("scala wordCount demo")
  }
}
