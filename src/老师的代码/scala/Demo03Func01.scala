package scala

object Demo03Func01 {
  /**
   * 函数的定义：
   * def main(args: Array[String]): Unit = {
   *
   * }
   *
   * def 关键字，声明一个函数
   * main 函数名，main方法比较特殊，是程序的主函数（入口）
   * 括号中都是函数能接收的参数
   * args: Array[String] 表示该函数能够接收一个类型是Array[String]参数
   * Unit：返回值类型，如果没有返回值则设为Unit
   * { 函数体，你写的逻辑 }
   */
  val pai: Double = 3.1415

  // 定义一个函数求圆的面积：Π * R * R
  def area(r: Double): Double = {
    val a: Double = pai * r * r
    return a
  }

  /**
   * 函数的省略规则：
   * 1、return可以省略，默认以最后一行代码最为返回值
   * 2、如果代码只有一行，花括号可以省略
   * 3、返回值类型可以自动推断，也可以省略
   * 4、如果函数没有参数，则可以省略括号
   *
   */
  def area01(r: Double): Double = {
    val a: Double = pai * r * r
    a
  }

  def area02(r: Double): Double = pai * r * r

  def area03(r: Double) = pai * r * r

  def area04 = pai * 2 * 2

  /**
   * 匿名函数：没有名字的函数，类似Python中的Lambda表达式
   * 匿名函数完整的结构：
   * (参数名:参数类型,参数2:参数2的类型,......) => { 函数体 }
   * Python：lambda 参数:函数体
   */

  def mFunc(i1: Int, i2: Int): Int = i1 + i2

  // 该函数可以接收一个参数f，参数f的类型为函数类型"(Int, Int) => Int"
  // 所接受的函数的类型：“能够接收两个Int类型的参数，返回一个Int类型的参数” 这样的一个函数
  def funcX(f: (Int, Int) => Int) = {
    println(f(10, 20))
  }


  def main(args: Array[String]): Unit = {
    // 调用函数
    println(area(2))

    funcX(mFunc)
    // mFunc的匿名函数写法
    funcX((i1: Int, i2: Int) => {
      i1 + i2
    })

    // 最终mFunc的简写形式
    funcX(_ + _)
  }

}
