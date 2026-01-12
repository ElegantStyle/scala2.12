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