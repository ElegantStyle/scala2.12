import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo09Cartesian {
  def main(args: Array[String]): Unit = {
    //创建Spark对象
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))

    val List01: List[Int] = List(1, 2, 3)
    val List02: List[Int] = List(2, 3, 4)
    val ListRDD1: RDD[Int] = sc.parallelize(List01)
    val ListRDD2: RDD[Int] = sc.parallelize(List02)
    ListRDD1.cartesian(ListRDD2).foreach(println)

  }
}
