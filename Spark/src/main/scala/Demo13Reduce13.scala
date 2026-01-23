import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo13Reduce13 {
  def main(args: Array[String]): Unit = {
    // 间接实现求平均值
    // 创建Spark对象
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))
    val ScoRDD: RDD[String] = sc.textFile("./data/Spark/score.csv")
    val SplitRDD: RDD[(String, Int)] = ScoRDD.map(sco => (sco.split(",")(0), sco.split(",")(2).toInt))

    // 计算每个学生的总分
    val SumRDD: RDD[(String, Int)] = SplitRDD.reduceByKey((i1, i2) => (i1 + i2))

    // 计算每个学生的科目数量
    val SizeRDD: RDD[(String, Int)] = SplitRDD.groupBy(score => score._1)
      .map(sc => (sc._1, sc._2.size))
    //SQL中的内连接
    val JoinRDD: RDD[(String, (Int, Int))] = SumRDD.join(SizeRDD)
    JoinRDD.map(sc=> {
      val key: String = sc._1
      val value: Int = sc._2._1 / sc._2._2
      (key,value)
    }).foreach(println)

  }
}
