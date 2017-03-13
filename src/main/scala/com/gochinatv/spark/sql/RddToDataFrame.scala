package com.gochinatv.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhuhuihui on 17/1/15.
  * 反射RDD的方法生成DataFrame
  */
case class User(name:String,age:Int)

object RddToDataFrame {


  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("RddToDataFrame").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //隐士转换从RDD转换为DataFrame
    import sqlContext.implicits._

    val rdd = sc.textFile("/Users/zhuhuihui/ruby-works/validate.rb").map(_.split(" "))

    val caseclass = rdd.map(p=>User(p(0),p(1).toInt))

    //转换成RDD
    val people = caseclass.toDF()
    people.show()

  }


}
