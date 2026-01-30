package com.streamting

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo04 {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local[*]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))
    ssc.checkpoint("/data/04_Streaming")

    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)

    // 统计每个单词的数量
    val WordKVDS: DStream[(String, Int)] = lineDS.flatMap(_.split(","))
      .map(word => (word, 1))

    WordKVDS.updateStateByKey((sep:Seq[Int],op:Option[Int])=>{
      val KVSum: Int = sep.sum
      val lastValue: Int = op match {
        case Some(lastBatchValue) => lastBatchValue
        case None => 0
      }
      Some(KVSum+lastValue)
    }).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }
}
