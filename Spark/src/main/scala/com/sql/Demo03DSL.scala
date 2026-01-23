package com.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo03DSL {
  def main(args: Array[String]): Unit = {
    /*
    * DSL:特定领域语言,这里的DSL只适用与SparkSQL的Dataframe
    * 常用的操作:select、where、groupBy(搭配agg进行聚合)、orderBy、limit、Union、join
    * 其他:withColumnRenamed、withColumn
    * */

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.replace("$", ""))
      .master("local")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val stuDF: DataFrame = spark.read
      .format("orc")
      .option("seq", "|")
      .load("./data/Spark/02_orc")

    //1.select
//    stuDF.select($"id").show()

    // 2.where条件
    // 筛选出文科四班的女生
//    stuDF.select("*")
//      .where($"clazz" === "文科四班" and $"gender"==="女")
//      .show()

    // 3.groupBy 分组和where的连用
    // 只看文科一班的人数
//    stuDF.select($"*")
//      .groupBy($"clazz")
//      .agg(count("*") as "cnt" )
//      .where($"clazz"==="文科一班")
//      .show()
    // order by 筛选
    // 按照每个班级人数进行降序,得到班级人数排名前5
//    stuDF.select($"*")
//      .groupBy($"clazz")
//      .agg(count("*") as "cnt")
//      .orderBy($"cnt".desc)
//      .limit(5)
//      .show()

    // union合并
    val stu2DF: DataFrame = spark.read
      .format("orc")
      .option("seq", "|")
      .load("./data/Spark/02_orc")

    // 去重
    val stuDFco: Long = stuDF.union(stu2DF).describe().count()
    println(stuDFco)
    // 不去重
    val stuDF2co: Long = stuDF.union(stu2DF).count()
    println(stuDF2co)


    // join 和limit在一起用

  }
}
