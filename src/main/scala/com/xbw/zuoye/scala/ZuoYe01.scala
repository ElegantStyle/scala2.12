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

    linesList.flatMap(word=>word.split(','))
      .groupBy(word=>word)
      .map(words=>{
        val key = words._1
        val value = words._2
        val count = value.size
        s"$key,$count"
      })
      .foreach(println)
  }
}