package com.streamting

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Durations, StreamingContext}

object Demo03 {
  def main(args: Array[String]): Unit = {

    // 创建SparkSeesion的环境
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local[2]")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Durations.seconds(5))

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val LineDS: ReceiverInputDStream[String] = ssc.socketTextStream("master", 8888)


    LineDS.foreachRDD(rdd=>{
      val df: DataFrame = rdd.toDF("line")
      df.select(explode(split($"line",",")) as "word")
        .groupBy($"word")
        .agg(count("*") as "cnt")
        .show()
    })

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
