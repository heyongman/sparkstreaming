package com.he

object FuncTest {
  def main(args: Array[String]): Unit = {
    curryingFunc(1)(2)
    val curry = curryingFunc(1)_
    curry(2)

    partialFunction()

    partialAppliedFunction()



  }


  /**
    * 柯里化函数
    * @param a
    * @param b
    */
  def curryingFunc(a:Int)(b:Int): Unit ={
    println(a+b)
  }

  /**
    * 偏函数
    * list的collect入参是一个PartialFunction，也可用case创建一个偏函数，两者等价
    */
  def partialFunction(): Unit ={
    val pf = new PartialFunction[Any, Int] {
      override def isDefinedAt(x: Any): Boolean = if (x.isInstanceOf[Int]) true else false

      override def apply(v1: Any): Int = v1.asInstanceOf[Int] + 1
    }

    val ints = List(1,2,3,"a").collect(pf)
    val ints1 = List(1,2,3,"a").collect({case i:Int => i+1})

    println(ints)
    println(ints1)
  }

  /**
    * 偏应用函数，也可以称作部分应用函数
    */
  def partialAppliedFunction(): Unit ={
    def add(x:Int,y:Int,z:Int) = x+y+z

    val addedX = add(1,_:Int,_:Int)
    val addedXY = add(1,2,_:Int)

    println(addedX(2, 3))
    println(addedXY(3))

  }


}
