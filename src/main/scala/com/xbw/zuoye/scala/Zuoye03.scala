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