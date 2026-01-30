package com.streamting

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo01 {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      // 流处理任务至少需要一个线程用于接收数据,所以这里需要设置线程为2
      .master("local[2]")
      .getOrCreate()

    // 创建SparkStreaming的入口
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))

    // 从socket接收实时的数据
    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)
    lineDS.flatMap(_.split(","))
      .map(word=>(word,1))
      .reduceByKey((a,b)=>a+b)
      .print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }
}
