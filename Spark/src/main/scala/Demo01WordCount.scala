import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01WordCount {
  def main(args: Array[String]): Unit = {
    // 1.构建Spark环境
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val Lines= sc.textFile("./data/Spark/word.txt")
    Lines.flatMap(line=>line.split(","))
      .groupBy(line=>line)
      .map(line=>(line._1,line._2.size))
      .foreach(println)
  }
}