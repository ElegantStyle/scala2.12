package scala

object Demo04Func02 {
  /**
   * Scala中的函数可以在哪里定义？
   * object中定义、class中定义、函数内部定义
   * 即：除了object以及class之外，在它们里面的任何位置都可以定义函数
   */

  /**
   * 面向对象编程：对象可以作为参数或者是返回值进行传递，参数以及返回值是有类型要求的
   * 面向函数编程：函数可以作为参数或者是返回值进行传递，同样，函数在传递时也需要考虑类型
   *
   * 函数的类型由什么决定？
   * 1、和def关键字无关
   * 2、和函数名无关
   * 3、和参数名无关
   * 4、和函数体无关
   * 就只剩 参数的类型 以及 返回值类型
   * 故：一个函数的类型是由该函数所接收的参数类型（可能有多个参数）以及返回值类型有关
   *
   * 怎么表示函数的类型？
   * (参数的类型) => 返回值类型
   * 如果只有一个参数，左边的括号可以省略
   *
   * 面向函数编程分为两种：
   * 1、以函数作为参数（重点掌握）
   * 2、以函数作为返回值（基本不用）
   *
   */

  /**
   * 匿名函数：
   * (参数名:参数类型,参数名:参数类型,......) => { 函数体 }
   */

  // (Int,Double) => Int
  def f1(a: Int, b: Double): Int = {
    b.toInt + a + 101
  }

  val a:Int = 2
  // 匿名函数其实也可以有名字
  val a_f1: (Int, Double) => Int = (a: Int, b: Double) => {
    b.toInt + a + 201
  }

  // (Double,Double) => Int
  def f2(a: Double, b: Double): Int = {
    b.toInt + a.toInt + 102
  }

  // (Int,Double) => Int
  def f3(a1: Int, b: Double): Int = {
    b.toInt + a1 + 103
  }

  // (Int,Double) => Double
  def f4(a: Int, b: Double): Double = {
    b.toInt + a + 104
  }

  // 定义一个函数fX，可以接收一个参数f，参数f的类型 是一种函数类型：(Int,Double) => Int，不要返回值
  // ((Int, Double) => Int) => Unit
  def fX(f: (Int, Double) => Int): Unit = {
    // 可以对接收到的函数f进行调用
    val i: Int = f(10, 200D)
    println(i)
  }

  def fXX(fx:((Int, Double) => Int) => Unit) = {
    fx(f1)
  }

  // main函数的类型：Array[String] => Unit
  def main(args: Array[String]): Unit = {
    fX(f1) // 311
    fX(a_f1) // 411

    /**
     * 匿名函数的省略：
     * 1、return关键字可以省略
     * 2、代码只有一行，函数体的花括号可以省略
     * 3、如果匿名函数是作为参数，传给另一个函数，则参数类型可以省略，注意：参数名不能省略
     * 4、如果参数只使用了一次，则可以用下划线替代，且 => 左边的部分都可以省略 （慎用）
     */

    // 匿名函数没有名字
    fX((a: Int, b: Double) => {b.toInt + a + 201}) // 411
    fX((a: Int, b: Double) => b.toInt + a + 201) // 411
    fX((a, b) => b.toInt + a + 201) // 411
    fX(_ + _.toInt + 201) // 411


    //    fX(f2)
    fX(f3) // 313
    //    fX(f4)
    fXX(fX) // 311
  }

}
