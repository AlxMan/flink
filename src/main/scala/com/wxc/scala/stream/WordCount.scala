package com.wxc.scala.stream

import org.apache.flink.streaming.api.scala._

object WordCountScalaStream {
  def main(args: Array[String]): Unit = {
    //处理流式数据
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val streamData: DataStream[String] = environment.socketTextStream("node51", 7000)
    val out = streamData.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    out.print()
    environment.execute()
  }

}