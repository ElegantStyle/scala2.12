package com.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo01SS {
  def main(args: Array[String]): Unit = {

    // 建立SparkSeesion对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local")
      .getOrCreate()

    // 完成单词统计
    // 第一种方法:使用SparkContext方法(RDD)

    val sc: SparkContext = spark.sparkContext
    sc.textFile("./data/Spark/word.txt")
      .flatMap((word: String) =>word.split(","))
      .groupBy(word=>word)
      .map(word=>word._2.size)
      .foreach(println)

    // 第二种方法:使用SparkSQL方法
    val LineSQL: DataFrame = spark.read
      // 表示读取csv的文件
      .format("csv")
      // 表示使用的分隔符号是|
      .option("sep", "|")
      // 表示给列名称为line
      .schema("line String")
      // 表示读取./data/Spark/word.txt
      .load("./data/Spark/word.txt")

    // 创建视图,视图名称为words
    LineSQL.createTempView("words")

    // 编写SQL
    spark.sql(
      """
        |select t1.word
        |       ,count(*) as cnt
        |from (
        |     select explode(split(line,",")) as word
        |     from words
        |) t1 group by t1.word
        |""".stripMargin).show(5)

    // 第三种方式DSL
    import spark.implicits._
    import org.apache.spark.sql.functions._

    LineSQL.select(explode(split($"line",",")) as "word")
      .groupBy($"word")
      .agg(count("*") as "cnt")
      .show()


  }
}

