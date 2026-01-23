import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo12ReudceByKey {
  def main(args: Array[String]): Unit = {
    // 创建Spark对象
    val sc = new SparkContext(new SparkConf()
      .setAppName(this.getClass.getSimpleName.replace("$", ""))
      .setMaster("local[2]"))

    val ScoRDD: RDD[String] = sc.textFile("./data/Spark/score.csv")
    val SplitRDD: RDD[(String, Int)] = ScoRDD.map(sco => (sco.split(",")(0), sco.split(",")(2).toInt))
    // 做sum,统计每个学生的总成绩
    SplitRDD.reduceByKey((i1,i2)=>i1+i2).take(5).foreach(println)
    println("="*50)
    // 求每个学生最高的成绩
    SplitRDD.reduceByKey((i1,i2)=>{
      val maxValue: Int = if(i1>i2){
        i1
      }else{
        i2
      }
      maxValue
    }).take(5).foreach(println)

    // 求每个学生最低的成绩
    SplitRDD.reduceByKey((i1,i2)=>{
      val minValue: Int = if(i1<i2){
        i1
      }else{
        i2
      }
      minValue
    }).take(10).foreach(println)

  }
}
