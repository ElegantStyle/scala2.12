package com.sql.zuoye

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CompanyRevenueAnalysis {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession 并配置
    val spark = SparkSession.builder()
      .appName("Company Revenue Analysis")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    import spark.implicits._
    
    // 读取CSV并转换为月度收入数据
    val monthlyDf = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .csv("./data/Spark/burks.csv")
      .select(
        col("_c0").as("company_code"),
        col("_c1").as("year"),
        expr("""stack(12, 1, _c2, 2, _c3, 3, _c4, 4, _c5, 5, _c6, 6, _c7, 
                7, _c8, 8, _c9, 9, _c10, 10, _c11, 11, _c12, 12, _c13) as (month, monthly_revenue)""")
      )
      .withColumn("month", col("month").cast("int"))

    // 任务1：每月累计收入
    println("=== 每个公司每年按月累计收入 ===")
    monthlyDf
      .withColumn("cumulative_revenue", 
        sum("monthly_revenue").over(
          Window.partitionBy("company_code", "year").orderBy("month")
        )
      )
      .orderBy("company_code", "year", "month")
      .select("company_code", "year", "month", "monthly_revenue", "cumulative_revenue")
      .show(30, truncate = false)

    // 任务2：同比增长率
    println("=== 每个公司当月比上年同期增长率 ===")
    monthlyDf
      .withColumn("last_year_revenue", 
        lag("monthly_revenue", 1).over(
          Window.partitionBy("company_code", "month").orderBy("year")
        )
      )
      .filter(col("last_year_revenue").isNotNull)
      .withColumn("growth_rate", (col("monthly_revenue") / col("last_year_revenue")) - 1)
      .orderBy("company_code", "year", "month")
      .select("company_code", "year", "month", "last_year_revenue", "monthly_revenue", "growth_rate")
      .show(30, truncate = false)

    // 任务3：环比增长率
    println("=== 环比增长率 ===")
    monthlyDf
      .withColumn("last_month_revenue", 
        lag("monthly_revenue", 1).over(
          Window.partitionBy("company_code", "year").orderBy("month")
        )
      )
      .filter(col("last_month_revenue").isNotNull)
      .withColumn("mom_growth_rate", (col("monthly_revenue") / col("last_month_revenue")) - 1)
      .orderBy("company_code", "year", "month")
      .select("company_code", "year", "month", "last_month_revenue", "monthly_revenue", "mom_growth_rate")
      .show(20, truncate = false)

    spark.stop()
  }
}