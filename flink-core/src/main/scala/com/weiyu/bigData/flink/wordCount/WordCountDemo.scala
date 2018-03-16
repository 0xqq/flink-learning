package com.weiyu.bigData.flink.wordCount

import java.io.File

import org.apache.flink.api.scala.ExecutionEnvironment

object WordCountDemo {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment(1)
    val text = env.readTextFile("E:"+File.separator+"test.txt")

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()

    env.execute("scala wordCount demo")
  }
}
