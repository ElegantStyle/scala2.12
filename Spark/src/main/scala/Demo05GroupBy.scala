import org.apache.spark.{SparkConf, SparkContext}

object Demo05GroupBy {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName.replace("$", "")))
    // 统计每个班级的人数
    sc.textFile("./data/Spark/students.csv")
      .map(line=>(line.split(",")(4),1))
      .groupBy(student=>student._1)
      .map(student=>(student._1,student._2.size))
      .foreach(println)

  }
}
