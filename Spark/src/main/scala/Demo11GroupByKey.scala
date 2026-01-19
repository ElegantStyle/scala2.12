import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo11GroupByKey {
  def main(args: Array[String]): Unit = {
    // 创建spark对象
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))
    // 读取学生数据
    val SparkRDD: RDD[String] = sc.textFile("./data/spark/students.csv")
    // 统计每个班级人数
    SparkRDD.map(stu => (stu.split(",")(4), stu.split(",")(3)))
      .groupByKey()
      .map(stu=>(stu._1,stu._2.size))
      .foreach(println)


  }
}
