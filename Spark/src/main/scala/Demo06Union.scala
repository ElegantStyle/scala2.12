import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo06Union {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val Union01: RDD[String] = sc.textFile("./data/Spark/students.csv")
    val Union02: RDD[String] = sc.textFile("./data/Spark/score.csv")
    Union01.union(Union02).foreach(println)

  }
}
