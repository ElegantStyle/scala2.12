package scala

import scala.collection.mutable.ListBuffer

object Demo02Base {
  def main(args: Array[String]): Unit = {
    // 单行注释
    /*
    多行注释
    多行注释
    多行注释
     */

    /**
     * 文档注释
     * 文档注释
     * 文档注释
     * 用于源代码中对方法或类的使用进行说明
     */

    /**
     * 变量：声明、命名规范、数据类型
     */

    // 定义两个int整形的变量，赋值10
    // 同python声明变量类似，可以不用指定类型，类型可以自动由赋的值进行推断（类型的自动推断）
    // 但是变量前面必须使用val 或者是 var进行声明
    // 如何选择val或var？ 原则：尽可能使用val
    // 命令规范：只能是数字、字母、下划线，必须以字母开头，不能是关键字

    val a = 10 // val声明的变量一旦赋值后，不能修改，即不能重新赋值，即它是一个常量
    //    a = 20 // 不能赋值，会报错，因为a是val修饰的
    println(a) // 在控制台输出 a 的值


    var b = 30 // var声明的变量可以重新赋值，但只能赋相同类型的值（静态数据类型）
    print(b) // 和println的差异在于 结尾是否有换行符
    b = 20
    println(b)

    // val修饰的变量就是不可变的
    // 下面的例子中为啥好像改变了？实际上变量的引用一直没变
    val list: ListBuffer[Int] = ListBuffer(1, 2, 3)
    println(list)
    println(list(1)) // 通过下标取元素
    list(1) = 22
    println(list)

    // 完整的变量声明
    val c: Int = 21 // 显示指定类型，当然可以省略
    println(c)

    /**
     * Scala中的基本数据类型：
     * 整形：Byte、Short、Int、Long，差异在于整数值的大小
     * 浮点：Float、Double
     * 字符：Char
     * 布尔：Boolean
     *
     * 实际上在Scala中所有类型都是引用类型
     * python中的引用类型有一个顶级父类object，java中也有object
     * 在Scala中对应AnyRef，然后AnyRef上面还有一个父类：Any
     * 在Scala中的“基本数据类型”有一个公共的父类：AnyVal，同AnyRef一致，Any是他们的父类
     */

    // 整形数据类型范围计算：-2^(n-1)   ~   2^(n-1)-1
    // n是位数，一个字节等于8位

    // Byte：一个字节
    val byte: Byte = 127 // 默认整数赋值后会自动以Int类型接收，需要显示指定（强制转换）
    println(byte)

    // Short：两个字节
    val short: Short = 10000
    println(short)

    // Int：四个字节
    val int: Int = 1000000000
    println(int)

    // Long：八个字节，值需要用L标记一下，表示Long类型，否则默认是Int类型
    val long: Long = 1000000000000000000L
    println(long)

    // Char字符：和Python有区别，Python没有字符的概念，只有字符串，而且Python中成对的单双引号都表示字符串
    // 在Scala中，单引号引起来的表示字符，双引号引起来的表示字符串
    val char: Char = 'a' // 字符只能是单个
    val str: String = "abc" // 多个字符是字符串，必须用双引号声明
    println(char)
    println(str)

    // String字符串：该类型实际上是来源于Java中的字符串，并不属于Scala的基本数据类型
    // 字符串常见的一些方法
    val wordsStr = "java,scala,python"

    val wordArr: Array[String] = wordsStr.split(",")
    println(wordArr) // 引用类型直接打印会看到内存地址

    // 传统的打印方式，类似Python中的for in
    for (word <- wordArr) {
      println(word)
    }
    // 使用面向函数的编程思想
    wordArr.foreach(println)

    println(wordsStr.substring(4, 11))

    println("=" * 20)
    // 字符串格式化
    // {} * {} = {}
    val i1 = 7
    val i2 = 3
    val i3: Int = i1 * i2
    println(i1 + " * " + i2 + " = " + i3)
    println(s"$i1 * $i2 = $i3")
    // 注意：如果变量名以下划线开头则必须加上花括号
    val _i4 = 66
    //    println(s"$_i4") // idea不会飘红，但无法运行
    println(s"${_i4}")

    // Float 四个字节 Double 八个字节
    val float: Float = 1.23F // Float类型需要用F修饰
    val double: Double = 1.2345678 // 默认小数在Scala中的Double类型
    println(float)
    println(double)

    // Boolean 一个字节：true、false
    val t: Boolean = true
    val f: Boolean = false
    println(t)
    println(f)

    /**
     * 结构：选择结构、循环结构
     */

    // 注意条件顺序，会依次从上往下判断
    val age = 20
    if (age >= 18) {
      println("成年")
    } else if (age >= 0) {
      println("未成年")
    } else {
      println("年龄必须大于0")
    }

    println("*" * 20)

    // 循环结构：while、for
    // for循环，同Python类似，主要用于遍历数据容器：Array、List、Map、Set、Tuple等等
    val ints: List[Int] = List(1, 2, 3, 4, 5)
    for (i <- ints) {
      println(i)
    }
    println("*" * 20)
    // for循环通常可以搭配range一起使用
    for (i <- Range(1, 10)) {
      println(i)
    }
    println("*" * 20)
    for (i <- 1 to 9) {
      println(i)
    }
    println("*" * 20)
    for (i <- 1 until 10) {
      println(i)
    }

    // while循环：while do、do while
    // 计算：1~100之和 5050
    var sum: Int = 0
    var cnt: Int = 0
    while (cnt < 100) { // 循环有可能一次都不执行
      cnt += 1
      sum += cnt
    }
    println(sum)

    sum = 0
    cnt = 0
    do { // do while 至少会执行依次循环
      cnt += 1
      sum += cnt
    } while (cnt < 100)
    println(sum)

    // 类型的转换：在Scala中要转什么类型就to类型
    // str to int
    val s1:String = "123"
    val str2int: Int = s1.toInt
    println(str2int)

    // Scala运算符：算数运算符、位运算符、布尔运算符
    // 算数：+ - * / %
    val ii1:Int = 10
    val ii2:Int = 4
    println(ii1 + ii2)
    println(ii1 - ii2)
    println(ii1 * ii2)
    println(ii1 / ii2) // 2 or 2.5 ？ 两个Int类型相除还会得到一个Int类型
    println(ii1.toDouble / ii2)  // 2.5
    println(ii1 % ii2) // 取余

    // 逻辑运算符：&& || ! ^
    println(true && true) // true
    println(true || true) // true
    println(true && false) // false
    println(true || false) // true
    println(false || false) // false
    println(true ^ false) // true
    println(false ^ false) // false
    println(true ^ true) // false
    println(!true) // false

    // 位运算符：<< >> & |
    println(8 << 2) // 32 00001000 00100000
    println(8 >> 2) // 2 00000010
    println(8 & 3) // 按位与 0 00001000 00000011
    println(8 | 3) // 按位或 11


  }
}
