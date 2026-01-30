package com.streamting

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo02 {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local[2]")
      .getOrCreate()


    // 创建streaming
    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))
    val lineDS: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)

    // 将Streaming每隔Duration大小的时间间隔所收到的数据转成RDD执行
    lineDS.foreachRDD(
      // 直接使用RDD的逻辑进行处理
      rdd=>{
        val DSrdd: RDD[(String, Int)] = rdd.flatMap(_.split(","))
          .map(word => (word, 1))
          .reduceByKey((a, b) => a + b)

        DSrdd.foreach(println)
      }
    )


    // 使用transform,需要有返回值
    lineDS.transform(
      rdd=>{
        val wordRDD: RDD[(String, Int)] = rdd.flatMap(_.split(","))
          .map(word => (word, 1))
          .reduceByKey((a,b) => a+b)
        wordRDD
      }
    ).print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
