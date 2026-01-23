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
  /*
  基于学生、分数、科目数据使用Scala语言完成下面的练习
  中括号为最终要求输出的格式
  */
    def main(args: Array[String]): Unit = {
    // 学生分数表
    val ScoreList = Source.fromFile("./data/score.csv")
    // 学生信息表
    val StuList= Source.fromFile("./data/students.csv")
    
    // 1.统计性别人数 [性别,人数]
    StuList
    .getLines()
    .toList
    .map(student=>student.split(',')(3))
    .groupBy(student=>student)
    .map(gender=>s"${gender._1},${gender._2.size}")
    .foreach(println)
    
    ScoreList.close()
    StuList.close()
    }
  }
  ```

  * 统计每个学生平均分 [学号,平均分]

    ```scala
    ScoreList
    .getLines()
    .toList
    .map(Score => {
      val SplitList = Score.split(",")
      val Split_1 = SplitList(0)
      val Split_2 = SplitList(2).toDouble
      (Split_1,Split_2)
    })
    .groupBy(score=>score._1)
    .map(score=>{
      val id = score._1
      val Score = score._2.map(ss=>ss._2)
      val avg = Score.sum / Score.size
      f"$id,${avg}%.2f"
    })
    .foreach(println)
    ```

* 统计每个学生的总分 [学号,学生姓名,学生年龄,总分]

  ```scala
  var ScoList = ScoreList
    .map(Score => {
      val SplitList = Score.split(",")
      val Split_1 = SplitList(0)
      val Split_2 = SplitList(2).toDouble
      (Split_1,Split_2)
    })
    .groupBy(score=>score._1)
    .map(score=>{
      val id = score._1
      val Score = score._2.map(ss=>ss._2)
      val sum = Score.sum
      (id,sum)
    })

  StuList
    .map(stu=>{
      val Splits = stu.split(",")
      val id = Splits(0)
      val name = Splits(1)
      val gender = Splits(2)
      val sumScore = ScoList.getOrElse(id,0)
      (id,name,gender,sumScore)
    }).foreach(println)
  ```

#### 阶段三

> 数据同阶段二一样
>
> 题目难度偏大

* 1、统计年级排名前十学生各科的分数 [学号,  姓名，班级，科目，分数]

  ```scala
  package com.xbw.zuoye.scala
  
  import scala.io.Source
  import scala.collection.mutable
  
  object Zuoye03 {
    def main(args: Array[String]): Unit = {
    // 学生分数表
    val SourceSco = Source.fromFile("./data/score.csv")
    val ScoreList = SourceSco.getLines().toList
    // 学生信息表
    val SourceStu=Source.fromFile("./data/students.csv")
    val StuList= SourceStu.getLines().toList
    // 科目表
    val SourceSub=Source.fromFile("./data/subject.csv")
    val SubList=SourceSub.getLines().toList
    
        // 1. 统计年级排名前十学生各科的分数 [学号, 姓名，班级，科目，分数]
        println("********************第一题******************************")
    
    
        val sort_10: List[String] = ScoreList.map(Stu_sco => {
            val Splits: Array[String] = Stu_sco.split(',')
            (Splits(0), Splits(1), Splits(2))
          })
          .groupBy(score => score._1)
          .map(score_sum => {
            val id: String = score_sum._1
            val totalScore: Double = score_sum._2.map(_._3.toDouble).sum
            (id, totalScore)
          }).toList
          .sortBy(-_._2)
          .take(10)
          .map(_._1).sorted
        println("排名前十的学生学号：" + sort_10)
    
        val stuMap: Map[String, (String, String)] = StuList
          .map(line => line.split(","))
          .filter(_.length >= 4)
          .map(arr => (arr(0), (arr(1), arr(4))))
          .toMap
    
    
        val subMap: Map[String, String] = SubList
          .map(line => line.split(","))
          .filter(_.length >= 2)
          .map(arr => (arr(0), arr(1)))
          .toMap
    
        val result = ScoreList
          .map(line => line.split(","))
          .filter(arr => sort_10.contains(arr(0)))
          .map(arr => {
            val stuId = arr(0)
            val subId = arr(1)
            val score = arr(2)
    
            val (name, clazz) = stuMap.getOrElse(stuId, ("未知姓名", "未知班级"))
            val subjectName = subMap.getOrElse(subId, "未知科目")
    
            (stuId, name, clazz, subjectName, score)
          })
    
        // 打印结果
        println("排名前十学生的各科分数：")
        result.foreach(item => {
          println(s"学号：${item._1}，姓名：${item._2}，班级：${item._3}，科目：${item._4}，分数：${item._5}")
        })
    
    
        // 关闭资源
        SourceSco.close()
        SourceStu.close()
        SourceSub.close()
    }
  }
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

