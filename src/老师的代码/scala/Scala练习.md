# Scala作业

#### 阶段一

> 需掌握Scala中常用集合List列表的使用

* 数据：

  ```
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  java,spark,java,hadoop
  ```

* 使用Scala语言完成WordCount统计

  ```scala
  package com.xbw.zuoye.scala
  
  object ZuoYe01 {
    def main(args: Array[String]): Unit = {
      val linesList: List[String] = List(
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop",
        "java,spark,java,hadoop"
      )
      val wordsList: List[String] = linesList.flatMap(line => line.split(",").toList)
      val groupedMap: Map[String, List[String]] = wordsList.groupBy(word => word)
      val wordCountMap: Map[String, Int] = groupedMap.map {
        case (word, wordList) => (word, wordList.size)
      }
  
      println("单词统计结果：")
      wordCountMap.foreach { case (word, count) =>
        println(s"$word: $count")
      }
    }
  }
  ```

#### 阶段二

> 基于学生、分数、科目数据使用Scala语言完成下面的练习
>
> 中括号为最终要求输出的格式
>
> 题目较为基础

* 统计性别人数 [性别,人数]

  ```scala
  package com.xbw.zuoye.scala
  
  import scala.io.Source
  
  object Zuoye02 {
    def main(args: Array[String]): Unit = {
      val studentPath = "./data/students.csv"
      val genderCount = countGender(studentPath)
      genderCount.foreach { case (gender, count) =>
        println(s"[$gender,$count]")
      }
    }
  
  
    def countGender(studentPath: String): Map[String, Int] = {
      Source.fromFile(studentPath)
        .getLines()
        .map(line => line.split(",")(3))
        .toList
        .groupBy(identity)
        .mapValues(_.size)
    }
  }
  ```

* 统计每个学生平均分 [学号,平均分]

  ```scala
  package com.xbw.zuoye.scala
  
  import scala.io.Source
  import scala.collection.mutable
  
  object Zuoye03 {
    def main(args: Array[String]): Unit = {
      val scorePath = "./data/score.csv"
      val studentAvgScore = calculateStudentAvgScore(scorePath)
      studentAvgScore.foreach { case (studentId, avg) =>
        println(s"[$studentId,${"%.2f".format(avg)}]")
      }
    }
  
    def calculateStudentAvgScore(scorePath: String): Map[String, Double] = {
      val scoreStats: mutable.Map[String, (Double, Int)] = mutable.Map.empty
      Source.fromFile(scorePath)
        .getLines()
        .foreach { line =>
          val parts = line.split(",")
          val studentId = parts(0)
          val score = parts(2).toDouble
  
          val (total, count) = scoreStats.getOrElse(studentId, (0.0, 0))
          scoreStats(studentId) = (total + score, count + 1)
        }
      scoreStats.map { case (id, (total, count)) =>
        (id, total / count)
      }.toMap
    }
  }
  ```

* 统计每个学生的总分 [学号,学生姓名,学生年龄,总分]

  ```scala
  
  ```

#### 阶段三

> 数据同阶段二一样
>
> 题目难度偏大

* 1、统计年级排名前十学生各科的分数 [学号,  姓名，班级，科目，分数]

  ```scala
  
  ```

* 2、统计总分大于年级平均分的学生 [学号，姓名，班级，总分]

  ```scala
  
  ```

* 3、统计每科都及格的学生 [学号，姓名，班级，科目，分数]

  ```scala
  
  ```

* 4、统计每个班级的前三名 [学号，姓名，班级，分数]

  ```scala
  
  ```

* 5、统计偏科最严重的前100名学生  [学号，姓名，班级，科目，分数]

  ```scala
  
  ```

