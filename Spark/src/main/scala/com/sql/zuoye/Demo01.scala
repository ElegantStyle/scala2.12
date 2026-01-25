package com.sql.zuoye

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo01 {
  def main(args: Array[String]): Unit = {
    /*
    * 手机号, 网格id, 城市id, 区县id, 停留时间, 起始时间, 结束时间,分区日期
    * mdn, grid_id, city_id, county_id, duration, start_time, end_time, pt
    * */


    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val dxDF: DataFrame = spark.read
      .format("csv")
      .option("seq", "|")
      .schema("mdn String, grid_id Bigint, city_id Bigint, county_id Bigint, duration Bigint , start_time String, end_time String, pt String")
      .load("./data/Spark/dianxin.csv")


    // 统计每座城市停留时间超过两小时的游客人数
    dxDF.select("*")
      .where($"duration" > 120)
      .groupBy($"city_id")
      .agg(count("*") as "cnt")
    //      .show()

    // 统计每座城市停留时间最久的前三个区县
    val city_county: WindowSpec = Window.partitionBy($"city_id").orderBy("county_id")
    dxDF.groupBy($"city_id", $"county_id")
      .agg(sum($"duration") as "total")
      .withColumn("rn", row_number() over city_county)
      .filter($"rn" <= 3)
      .select("*")
      .show()
  }
}
