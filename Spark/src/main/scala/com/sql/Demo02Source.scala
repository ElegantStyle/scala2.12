package com.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Demo02Source {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local")
      .getOrCreate()

    // CSV文件的写入读取
    val SpCSV: DataFrame = spark.read
      .format("csv")
      .option("seq", "|")
      .schema("id String,name String,age Int,gender String,clazz String")
      .load("./data/Spark/students.csv")
    // 默认打印前20行
    SpCSV.show()

    //    SpCSV.write.format("json").mode(SaveMode.Overwrite).save("./data/Spark/01_json")
    //    SpCSV.write.format("orc").mode(SaveMode.Overwrite).save("./data/Spark/02_orc")
    //    SpCSV.write.format("parquet").mode(SaveMode.Overwrite).save("./data/Spark/03_parquet")



  }
}
