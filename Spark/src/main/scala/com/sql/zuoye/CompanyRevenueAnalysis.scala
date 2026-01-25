package com.sql.zuoye

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object CompanyRevenueAnalysis {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName("Company Revenue Analysis")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // 1. 读取 CSV 文件
    val df = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .option("spark.sql.shuffle.partitions","2")
      .csv("./data/Spark/burks.csv")

    // 2. 重命名列
    val dfWithNames = df.select(
      col("_c0").as("company_code"),
      col("_c1").as("year"),
      col("_c2").as("tsl01"), col("_c3").as("tsl02"), col("_c4").as("tsl03"),
      col("_c5").as("tsl04"), col("_c6").as("tsl05"), col("_c7").as("tsl06"),
      col("_c8").as("tsl07"), col("_c9").as("tsl08"), col("_c10").as("tsl09"),
      col("_c11").as("tsl10"), col("_c12").as("tsl11"), col("_c13").as("tsl12")
    )

    val monthlyExpr = """
      stack(12,
        1, tsl01, 2, tsl02, 3, tsl03, 4, tsl04,
        5, tsl05, 6, tsl06, 7, tsl07, 8, tsl08,
        9, tsl09, 10, tsl10, 11, tsl11, 12, tsl12
      ) as (month, monthly_revenue)
    """

    val monthlyDf = dfWithNames.select(
      col("company_code"),
      col("year"),
      expr(monthlyExpr)
    ).withColumn("month", col("month").cast("int"))

    // 任务1：统计每个公司每年按月累计收入（移除了rowsBetween）
    val windowSpec1 = Window.partitionBy("company_code", "year")
      .orderBy("month")

    val result1 = monthlyDf
      .withColumn("cumulative_revenue",
        sum("monthly_revenue").over(windowSpec1))
      .orderBy("company_code", "year", "month")

    println("=== 每个公司每年按月累计收入 ===")
    result1.select(
      "company_code", "year", "month", "monthly_revenue", "cumulative_revenue"
    ).show(30, truncate = false)

    // 任务2：统计每个公司当月比上年同期增长率
    val windowSpec2 = Window.partitionBy("company_code", "month")
      .orderBy("year")

    val result2 = monthlyDf
      .withColumn("last_year_monthly_revenue",
        lag("monthly_revenue", 1).over(windowSpec2))
      .filter(col("last_year_monthly_revenue").isNotNull)
      .withColumn("growth_rate",
        (col("monthly_revenue") / col("last_year_monthly_revenue")) - 1)
      .orderBy("company_code", "year", "month")

    println("=== 每个公司当月比上年同期增长率 ===")
    result2.select(
      "company_code", "year", "month",
      "last_year_monthly_revenue", "monthly_revenue", "growth_rate"
    ).show(30, truncate = false)

    // 计算环比增长率
    val windowSpec3 = Window.partitionBy("company_code", "year")
      .orderBy("month")

    val result3 = monthlyDf
      .withColumn("last_month_revenue",
        lag("monthly_revenue", 1).over(windowSpec3))
      .filter(col("last_month_revenue").isNotNull)
      .withColumn("mom_growth_rate",
        (col("monthly_revenue") / col("last_month_revenue")) - 1)
      .orderBy("company_code", "year", "month")

    println("=== 环比增长率 ===")
    result3.select(
      "company_code", "year", "month",
      "last_month_revenue", "monthly_revenue", "mom_growth_rate"
    ).show(20, truncate = false)
  }
}