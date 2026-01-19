import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo10MapValues {
  def main(args: Array[String]): Unit = {
    //建立一个Spark对象
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))

    val List01: List[(String, Int)] = List("key1" -> 1, "key2" -> 2, "key3" -> 3)
    val ListRDD: RDD[(String, Int)] = sc.parallelize(List01)
    ListRDD.mapValues(i=>i*i).foreach(println)

  }
}
