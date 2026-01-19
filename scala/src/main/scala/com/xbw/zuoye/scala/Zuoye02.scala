package com.xbw.zuoye.scala

import scala.io.Source

object Zuoye02 {
/*
基于学生、分数、科目数据使用Scala语言完成下面的练习
中括号为最终要求输出的格式
*/
def main(args: Array[String]): Unit = {
  // 学生分数表
  val SourceSco = Source.fromFile("./data/score.csv")
  val ScoreList = SourceSco.getLines().toList
  // 学生信息表
  val SourceStu=Source.fromFile("./data/students.csv")
  val StuList= SourceStu.getLines().toList

  println("*"*30)
  // 1.统计性别人数 [性别,人数]
  StuList
    .map(student=>student.split(',')(3))
    .groupBy(student=>student)
    .map(gender=>s"${gender._1},${gender._2.size}")
    .foreach(println)

  println("*"*30)
  // 2. 统计每个学生平均分 [学号,平均分]
  ScoreList
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
      f"$id,$avg%.2f"
    })
    .foreach(println)


  // 3. 统计每个学生的总分 [学号,学生姓名,学生年龄,总分]
  println("*"*30)
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


  SourceSco.close()
  SourceStu.close()

  }
}