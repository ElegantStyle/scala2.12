import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo08Sample {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))

    // 读取students.csv文件的数据
    val StuRDD: RDD[String] = sc.textFile("./data/Spark/students.csv")

    // 使用抽样函数进行抽样,seed不写死，默认是每次不一样
    println("第一次抽样")
    StuRDD.sample(withReplacement = true,fraction = 0.02).foreach(println)
    println("第二次抽样")
    StuRDD.sample(withReplacement = true,fraction = 0.02).foreach(println)
    // 使用抽样函数进行抽样,seed写死,是每次一样
    println("第三次抽样")
    StuRDD.sample(withReplacement = true, fraction = 0.02, seed = 1).foreach(println)
    println("第四次抽样")
    StuRDD.sample(withReplacement = true, fraction = 0.02, seed = 1).foreach(println)

  }
}
