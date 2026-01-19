import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo03flatMap {
  def main(args: Array[String]): Unit = {
    //将一个列表打印出来
    val TestList: List[String] = List("java", "vue", "Python")


    // 创建Spark的环境
    val conf: SparkConf = new SparkConf().setMaster("local[2]")
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
    val sc = new SparkContext(conf)
    val FlatRDD: RDD[String] = sc.parallelize(TestList)
    FlatRDD.flatMap(word=>word).foreach(println)
  }
}
