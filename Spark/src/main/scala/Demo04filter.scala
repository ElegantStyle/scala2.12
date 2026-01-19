import org.apache.spark.{SparkConf, SparkContext}

object Demo04filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ints: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    sc.parallelize(ints).filter(i=>i%2==0).foreach(println)
  }
}
