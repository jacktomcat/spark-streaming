package com.gochinatv.spark.streaming

/**
  * Created by tingyun on 2017/4/6.
  */
class Student {
  var age = 0
  val name="zhuhh"

}

object  Test{

  def main(args: Array[String]): Unit = {
    val student = new Student();
    // 赋值时调用student.age的set方法   def age_=(x$1: Int): Unit
    student.age = 20
    // 调用student.age，其实上调用get方法：def age: Int
    println(student.age);
  }

}
