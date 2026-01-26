package com.streamting

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo04UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    // 有状态算子：updateStateByKey，可以基于历史结果更新当前批次的结果

    val spark: SparkSession = SparkSession
      .builder()
      .appName("Demo04UpdateStateByKey")
      .master("local[2]") // Streaming任务需要至少一个线程用于接收数据，所以至少需要分配两个线程
      .getOrCreate()

    val ssc: StreamingContext = new StreamingContext(spark.sparkContext, Durations.seconds(5))
    // 如果使用有状态算子，则需要设置CheckPoint的路径
    ssc.checkpoint("./data/ssc")

    val lineDS: DStream[String] = ssc.socketTextStream("master", 8888)

    // 统计每个单词的数量
    val wordsDS: DStream[String] = lineDS.flatMap(_.split(","))

    val wordKVDS: DStream[(String, Int)] = wordsDS.map(word => (word, 1))

    wordKVDS
      // 每隔5秒，对相同key的数据进行聚合，并且会考虑历史的结果
      .updateStateByKey((seq: Seq[Int], op: Option[Int]) => {
        // seq: 批次中相同key对应的所有value
        val kVSum: Int = seq.sum // 对当前批次中某个Key的所有Value进行sum
        // op：上一次批次相同key对应的value
        val lastValue: Int = op match {
          case Some(lastBatchValue) => lastBatchValue
          case None => 0
        }
        Some(kVSum + lastValue)
      }).print()

    // 启动streaming任务
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()

  }

}
