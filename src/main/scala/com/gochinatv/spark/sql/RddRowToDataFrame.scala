package com.gochinatv.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhuhuihui on 17/1/15.
  * 程序动态从RDD生成DataFrame
  */
object RddRowToDataFrame {

  //1:从原来的RDD创建一个新的RDD,成员是Row类型,包含所有列。
  //2:创建一个StructType类型的表模式,其结构与1中创建的RDD的Row结构相匹配
  //3:使用SQLContext.createDataFrame方法将表模式应用到步骤1创建的RDD上
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("RddRowToDataFrame").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //一个普通的RDD
    val rdd = sc.textFile("/Users/zhuhuihui/ruby-works/validate.rb").map(_.split(" "))

    //字符串格式的表模式
    val schemaString = "name age"

    //依据字符串格式的表模式创建结构化的表模式,用StructType
    val scheme=StructType(schemaString.split(" ").map(filedName=>StructField(filedName,StringType,true)))

    val rowRdd = rdd.map(p=>Row(p(0),p(1).trim))
    val df  = sqlContext.createDataFrame(rowRdd,scheme)//将模式作用到RDD上,生成DataFrame
    df.show()

  }
}
