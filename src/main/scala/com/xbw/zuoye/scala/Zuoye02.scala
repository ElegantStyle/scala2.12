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