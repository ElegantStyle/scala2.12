import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo02Map {
  def main(args: Array[String]): Unit = {
    // 求一组数字的平方
    val Ints: List[Int] = List(1, 3, 4, 6, 7, 11, 2)

    // 建立Spark的环境
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
    val sc = new SparkContext(conf)

    val ListRDD: RDD[Int] = sc.parallelize(Ints)
    ListRDD.map(i => i*i).foreach(println)

  }
}
