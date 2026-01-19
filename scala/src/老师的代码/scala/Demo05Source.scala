import scala.collection.mutable.ListBuffer
import scala.io.{BufferedSource, Source}

object Demo05Source {
  def main(args: Array[String]): Unit = {
    // Scala读取文件
    val bs: BufferedSource = Source.fromFile("./data/stu/student.csv")
    // 迭代器只能迭代一次，出于内存考虑，数据并不会被加载到内存中，而是以流的形式传递
    val linesIter: Iterator[String] = bs.getLines()
    //    for (line <- linesIter) {
    //      println(line)
    //    }
    //    println("#" * 20)
    //    for (line <- linesIter) {
    //      println(line)
    //    }
    // 如果转成Scala中的数据容器，例如List，则相当于把所有数据加载到内存，注意文件不能太大
    // 转成List之后会有更多的好用的方法
    val linesList: List[String] = linesIter.toList
    // List是可以遍历多次的
    //    for (line <- linesList) {
    //      println(line)
    //    }
    //    println("#" * 20)
    //    for (line <- linesList) {
    //      println(line)
    //    }

    // 统计班级的人数
    val lb = ListBuffer[(String,Int)]()
    for (line <- linesList) {
      val clazz: String = line.split(",")(4)
      lb.append((clazz,1))
    }

    def mySplit(line:String):(String,Int) = {
      val clazz: String = line.split(",")(4)
      (clazz,1)
    }



    def myGrp(t2:(String,Int)):String = {
      t2._1 // 取二元组的第一个元素
    }

    def myCnt(kv:(String, ListBuffer[(String, Int)])):Unit = {
      val clazz: String = kv._1
      val values: ListBuffer[(String, Int)] = kv._2
      println(s"$clazz,${values.size}")
    }

    // 对myCnt用匿名函数进行简化
    val a_myCnt: ((String, ListBuffer[(String, Int)])) => Unit = (kv:(String, ListBuffer[(String, Int)])) => {
      val clazz: String = kv._1
      val values: ListBuffer[(String, Int)] = kv._2
      println(s"$clazz,${values.size}")
    }

    // 指定按什么进行分组
    //    for (kv <- lb.groupBy(myGrp)) {
    //      val clazz: String = kv._1
    //      val values: ListBuffer[(String, Int)] = kv._2
    //      println(s"$clazz,${values.size}")
    //    }

//    lb.groupBy(myGrp).foreach(a_myCnt)
    lb.groupBy(t2 => t2._1)  // 取二元组的第一个元素
     .foreach(kv => {
      val clazz: String = kv._1
      val values: ListBuffer[(String, Int)] = kv._2
      println(s"$clazz,${values.size}")
    })

    // 链式调用
    Source
      .fromFile("./data/stu/student.csv")
      .getLines()
      .toList
      .map(mySplit)
      .groupBy(t2 => t2._1)  // 取二元组的第一个元素
      .foreach(kv => {
        val clazz: String = kv._1
        val values: List[(String, Int)] = kv._2
        println(s"$clazz,${values.size}")
      })

  }

}
