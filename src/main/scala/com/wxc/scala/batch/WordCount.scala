package com.wxc.scala.batch

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

object WordCountScalaBatch {
  def main(args: Array[String]): Unit = {
    val inputPath = "data/wordcount.txt"
    val outputPath = "output/wordcount_scala.csv"
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val text: DataSet[String] = environment.readTextFile(inputPath)
    val out: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    out.writeAsCsv(outputPath,"\n", " ",FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    environment.execute("scala batch process")
  }
}
