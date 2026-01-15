package com.xbw.zuoye.scala

import scala.io.{BufferedSource, Source}
import scala.math.abs

object zuoye04 {
  // 定义样例类
  case class Student(Id: String, Name: String, Age: Int, Gender: String, Clazz: String)
  case class Score(Id: String, SubId: String, Score: Int)
  case class Subject(SubId: String, SubName: String, MaxScore: Int)

  // 全局变量（实际开发中建议使用局部变量，这里保持你的风格）
  var StuLines: List[Student] = _
  var ScoLines: List[Score] = _
  var SubLines: List[Subject] = _

  // 读取文件数据，返回三个数据集
  def readFile(): (List[Student], List[Score], List[Subject]) = {
    // 学生数据处理
    val StuFile: BufferedSource = Source.fromFile("./data/students.csv")
    StuLines = StuFile.getLines().toList.map(student => {
      val Splits: Array[String] = student.split(',')
      Student(Splits(0), Splits(1), Splits(2).toInt, Splits(3), Splits(4))
    })

    // 学生分数数据处理
    val SouFile: BufferedSource = Source.fromFile("./data/score.csv")
    ScoLines = SouFile.getLines().toList.map(score => {
      val Splits: Array[String] = score.split(',')
      Score(Splits(0), Splits(1), Splits(2).toInt)
    })

    // 科目信息数据处理
    val SubFile: BufferedSource = Source.fromFile("./data/subject.csv")
    SubLines = SubFile.getLines().toList.map(subject => {
      val Splits: Array[String] = subject.split(',')
      Subject(Splits(0), Splits(1), Splits(2).toInt)
    })

    // 关闭文件流
    SubFile.close()
    SouFile.close()
    StuFile.close()

    (StuLines, ScoLines, SubLines)
  }

  // 2.统计总分大于年级平均分的学生 [学号，姓名，班级，总分]
  def avg_example02(): Unit = {
    println("===== 2. 总分大于年级平均分的学生 =====")
    // 读取数据
    val (students, scores, _) = readFile()

    // 第一步：计算每个学生的总分
    val studentTotalScores = scores
      .groupBy(_.Id) // 按学号分组
      .map { case (stuId, scoreList) =>
        (stuId, scoreList.map(_.Score).sum) // (学号, 总分)
      }.toList

    // 第二步：计算年级平均分
    val totalAllStudents = studentTotalScores.map(_._2).sum
    val avgTotalScore = if (studentTotalScores.nonEmpty) totalAllStudents.toDouble / studentTotalScores.size else 0.0

    // 第三步：筛选出总分大于年级平均分的学生
    val qualifiedStudents = studentTotalScores.filter(_._2 > avgTotalScore)

    // 第四步：关联学生信息，输出结果
    val result = qualifiedStudents.map { case (stuId, totalScore) =>
      val student = students.find(_.Id == stuId).get
      (stuId, student.Name, student.Clazz, totalScore)
    }

    // 打印结果
    result.foreach { case (id, name, clazz, total) =>
      println(s"学号：$id, 姓名：$name, 班级：$clazz, 总分：$total")
    }
    println(s"年级平均分：$avgTotalScore")
    println("----------------------------------------\n")
  }

  // 3.统计每科都及格的学生 [学号，姓名，班级，科目，分数]
  // 及格标准：分数 >= 60（假设满分100，可根据实际调整）
  def pass_all_subjects(): Unit = {
    println("===== 3. 每科都及格的学生 =====")
    val (students, scores, subjects) = readFile()
    val passScore = 60

    // 第一步：按学号分组，检查每个学生的所有科目是否都及格
    val studentsWithAllPass = scores
      .groupBy(_.Id) // 按学号分组
      .filter { case (_, scoreList) =>
        scoreList.forall(_.Score >= passScore) // 所有科目分数 >= 60
      }.keys.toList // 得到所有每科都及格的学生学号

    // 第二步：关联学生信息和科目信息，输出详细数据
    val result = scores
      .filter(score => studentsWithAllPass.contains(score.Id)) // 筛选出及格学生的分数
      .map { score =>
        val student = students.find(_.Id == score.Id).get
        val subject = subjects.find(_.SubId == score.SubId).getOrElse(Subject(score.SubId, "未知科目", 100))
        (score.Id, student.Name, student.Clazz, subject.SubName, score.Score)
      }

    // 打印结果
    result.foreach { case (id, name, clazz, subName, score) =>
      println(s"学号：$id, 姓名：$name, 班级：$clazz, 科目：$subName, 分数：$score")
    }
    println("----------------------------------------\n")
  }

  // 4.统计每个班级的前三名 [学号，姓名，班级，总分]
  def top3_per_class(): Unit = {
    println("===== 4. 每个班级的前三名 =====")
    val (students, scores, _) = readFile()

    // 第一步：计算每个学生的总分
    val studentTotalScores = scores
      .groupBy(_.Id)
      .map { case (stuId, scoreList) =>
        val total = scoreList.map(_.Score).sum
        val clazz = students.find(_.Id == stuId).map(_.Clazz).getOrElse("未知班级")
        (stuId, total, clazz)
      }.toList

    // 第二步：按班级分组，每组内按总分降序排序，取前三名
    val result = studentTotalScores
      .groupBy(_._3) // 按班级分组
      .map { case (clazz, stuScores) =>
        // 按总分降序排序，取前三名
        val top3 = stuScores.sortBy(-_._2).take(3)
        (clazz, top3)
      }

    // 第三步：关联学生信息，输出结果
    result.foreach { case (clazz, top3List) =>
      println(s"班级：$clazz")
      top3List.zipWithIndex.foreach { case ((stuId, total, _), index) =>
        val student = students.find(_.Id == stuId).get
        println(s"  第${index + 1}名：学号=$stuId, 姓名=${student.Name}, 总分=$total")
      }
    }
    println("----------------------------------------\n")
  }

  // 5.统计偏科最严重的前100名学生 [学号，姓名，班级，科目，分数]
  // 偏科定义：单科目分数与该学生所有科目平均分的差值绝对值最大（差值越 大，偏科越严重）
  def most_unbalanced_100(): Unit = {
    println("===== 5. 偏科最严重的前100名学生 =====")
    val (students, scores, subjects) = readFile()

    // 第一步：计算每个学生的所有科目平均分
    val studentAvgScores = scores
      .groupBy(_.Id)
      .map { case (stuId, scoreList) =>
        val avg = scoreList.map(_.Score).sum.toDouble / scoreList.size
        (stuId, avg)
      }

    // 第二步：计算每个学生每科分数与平均分的差值绝对值（偏科程度）
    val unbalancedScores = scores.map { score =>
      val stuAvg = studentAvgScores(score.Id)
      val unbalancedDegree = abs(score.Score - stuAvg) // 偏科程度（绝对值）
      (score.Id, score.SubId, score.Score, unbalancedDegree)
    }

    // 第三步：按偏科程度降序排序，取前100条
    val top100Unbalanced = unbalancedScores.sortBy(-_._4).take(100)

    // 第四步：关联学生和科目信息，输出结果
    val result = top100Unbalanced.map { case (stuId, subId, score, degree) =>
      val student = students.find(_.Id == stuId).get
      val subject = subjects.find(_.SubId == subId).getOrElse(Subject(subId, "未知科目", 100))
      (stuId, student.Name, student.Clazz, subject.SubName, score, degree)
    }

    // 打印结果
    result.zipWithIndex.foreach { case ((id, name, clazz, subName, score, degree), index) =>
      println(s"第${index + 1}名：学号=$id, 姓名=$name, 班级=$clazz, 科目=$subName, 分数=$score, 偏科程度=$degree")
    }
    println("----------------------------------------\n")
  }

  // 主函数：执行所有统计功能
  def main(args: Array[String]): Unit = {
    // 2. 统计总分大于年级平均分的学生
    avg_example02()

    // 3. 统计每科都及格的学生
    pass_all_subjects()

    // 4. 统计每个班级的前三名
    top3_per_class()

    // 5. 统计偏科最严重的前100名学生
    most_unbalanced_100()
  }
}