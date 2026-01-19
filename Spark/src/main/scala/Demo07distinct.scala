import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo07distinct {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // 连接两个学生信息表
    val StuRDD01: RDD[String] = sc.textFile("./data/Spark/students.csv")
    val StuRDD02: RDD[String] = sc.textFile("./data/Spark/students.csv")

    StuRDD01.union(StuRDD02).distinct().foreach(println)

  }
}
