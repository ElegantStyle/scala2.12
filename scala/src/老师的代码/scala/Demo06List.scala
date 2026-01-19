object Demo06List {
  def main(args: Array[String]): Unit = {
    /**
     * Scala中的数据容器：List、Tuple、Map、Set
     * 对应Python：list、tuple、dict、set
     *
     * List：列表
     * 不可变的（一旦创建后不可以修改里面的元素，也不能增加或删除元素）
     * 元素可以重复，一般的List只存放同类型的元素，如果需要同时存储不同类型的元素，则可以将泛型指定为Any（公共的父类）
     * 有序：可以通过index下标取对应位置的元素
     */

    // 创建List
    val list01: List[Int] = List(1, 2, 3, 6, 7, 8, 4, 5, 2, 2, 3)
    println(list01)

    val list02: List[Double] = List[Double](1, 2, 3, 4, 1.1, 1.23, 4, 5)
    println(list02) // 整体打印
    list02.foreach(println) // 逐个元素打印

    // List常用的方式方法

    // 通过下标取元素
    println(list01(5))
    //    list01(5) = 88 // 不能对元素进行修改，List是不可变的
    println("*" * 100)
    println(list01)
    // List常用的方法
    println(list01.max)
    println(list01.min)
    println(list01.sum)
    println(list01.size)
    println(list01.length)
    println(list01.head) // 取第一个元素
    println(list01.tail) // 取第一个以外的元素
    println(list01.reverse) // 反转
    println(list01.distinct) // 去重
    println(list01.isEmpty) // 判断是否为空
    println(list01.slice(2, 5)) // 切片
    println(list01.contains(8))
    println(list01.take(6)) // 取前6个元素
    println(list01.drop(2)) // 去除前2个元素再返回
    // 以上所有操作都是返回一个新的值或者是列表
    println(list01) // 同第一次打印一致，所有的操作都不会对List本身产生影响（不可变）

    // List高级方法：map、flatMap、filter、foreach、sort相关的操作，重点掌握

    /**
     * map方法：可以将List中的每一个元素进行处理，并且会返回一个值
     * 传入一条数据 需要返回一条数据
     * 最终会将每个返回的元素组装成List再返回
     */

    val list03: List[Int] = List(1, 2, 3, 4, 5)
    // 对list03中的每一个元素进行平方再返回
    val new_list03: List[Int] = list03.map(i => i * i)
    println(new_list03)

    /**
     * flatMap方法：同map任务类似，也会对List中的每一个元素进行处理，每处理一个元素也需要有一个返回值
     * 只不过返回值类型必须是一种数据容器(String、Array、List等等)
     * 传入一行，返回N行
     * 类似SQL中的explode函数
     */

    // 需要将该List中的每个单词提取出来，并转成一个单词一行数据
    val wordsList: List[String] = List("java,scala,python", "hadoop,hive,spark")

    wordsList.map(line => {
      line.split(",").toList
    }).foreach(println)
    wordsList.flatMap(line => {
      line.split(",")
    }).foreach(println)

    /**
     * filter：用于过滤，需要接收一个函数类型的参数f，该函数f的返回值类型必须是Boolean类型
     * 函数f如果返回true则保留元素，为false则过滤掉
     */

    // 将奇数过滤掉，只保留偶数
    // return在Scala的语义中表示程序结束，一般不使用return返回数据
    val list04: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9)
    list04.filter(i => i % 2 == 0).foreach(println)

    /**
     * foreach：同map非常非常类似，也会对List中的每一个元素进行处理，只不过没有返回值（返回值必须是Unit）
     * 一般用于最后的遍历输出（打印、写文件、写数据库、保存到其他外部系统）
     */
    list01.foreach(println)

    /**
     * sort相关：sorted、sortBy、sortWith
     * sorted：直接从小到大排序
     * sortBy：指定元素为多维时，按那个维度进行排序
     * sortWith：指定一个排序规则
     */
    val list05: List[Int] = List(1, 8, 5, 6, 4, 2, 3, 9)

    // 从小到大排序
    println(list05.sorted) // 只能从小到大排序

    // 从大到小排序
    println(list05.sorted.reverse) // 先从小到大再反转

    val t3List: List[(String, Int, String)] = List(("zs", 95, "a"), ("ls", 90,"c"), ("ww", 99,"b"))
    println(t3List.sorted) // 对于多维元素，只会取第一个维度进行从小到大的排序
    // 按照分数降序排列
    println(t3List.sortBy(t3 => t3._2)) // 分数升序
    println(t3List.sortBy(t3 => -t3._2)) // 分数降序

    // 按最后的字母降序排列
    println(t3List.sortBy(t3 => t3._3)) // 升序
    println(t3List.sortBy(t3 => t3._3).reverse) // 降序
    println(t3List.sortBy(t3 => -t3._3(0).toInt)) // 降序

    val s:String = "abc"
    // 获取首字母的ascii
    val c: Char = s(0)
    println(c)
    println(c.toInt)

    val stuMap: Map[String, Int] = Map(("zs", 10), ("ls", 9), ("ww", 8))
    // 自定义排序规则：zs排第一，ls排第二，ww排第三
    // 接收两个参数，相当于同时会传入两条数据，你需要进行比较，最后通过一个Boolean决定两者是否交换位置
    println(t3List.sortWith((t31,t32)=>{
      t31._2 > t32._2
    }))


    println(t3List.sortWith((t31,t32)=>{
      stuMap.getOrElse(t31._1,0) > stuMap.getOrElse(t32._1,0)
    }))


  }

}
