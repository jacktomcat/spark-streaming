package com.gochinatv.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhuhuihui on 17/1/7.
  */
object SparkContextUtils {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)

    val textRdd = sparkContext.textFile("/Users/zhuhuihui/ruby-works/validate.rb")

    val arraySeq = Array(100,200,101,201,200)
    val arrayRdd = sparkContext.parallelize(arraySeq)
    val listRdd = sparkContext.makeRDD(List(100,500,300,400))

    val count = arrayRdd.count()
    val max = arrayRdd.max()
    val min = arrayRdd.min()
    val first = arrayRdd.first()

    val array = arrayRdd.collect()

    println("max="+max)
    println("min="+min)
    println("first="+first)
    println("count="+count)

    array.foreach(x=> println(x))
    println("=========================")
    arrayRdd.distinct().collect().foreach(x=>println("distinct= " + x))

    arrayRdd.union(listRdd).distinct().sortBy(x=>x+1,false).collect().foreach(x=>println("sort,distinct= " + x))
    arrayRdd.subtract(listRdd).foreach(x=>println("subtract= " + x))
    //arrayRdd.saveAsTextFile("/Users/zhuhuihui/array_rdd.txt")//保存之前是文件夹,文件夹中是分区文件信息
    arrayRdd.map(x=>x+1).foreach(x=>println("map="+x))
    arrayRdd.map(x=>(x,1)).foreach(x=>println("map="+x))

    arrayRdd.filter(x=>x>=101).foreach(x=>println("filter="+x))

    println(arrayRdd.countByValue().foreach(e => {val (k,v) = e ;println("countByValue="+k+":"+v)}))//按值去重复

    arrayRdd.groupBy(x=>(x+10)).foreach(x=>println("groupBy="+x))

    arrayRdd.keyBy(x=>(x+10)).foreach(x=>println("keyBy="+x))

    println("reduce="+arrayRdd.reduce((a,b)=>a+b))//里面的元素相加

    println("textRdd..count ="+textRdd.count())
    println("textRdd..flatMap..count = "+textRdd.flatMap(line=>(line.split(" "))).count())
    val rk = textRdd.flatMap(line=>(line.split(" "))).map(world=>(world,1)).reduceByKey((a,b)=>a+b)
    rk.foreach(x=>println("flatMap,reduceByKey==="+x))
  }

}
