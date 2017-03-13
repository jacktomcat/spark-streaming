package com.gochinatv.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

import scala.beans.BeanProperty

/**
  * Created by zhuhuihui on 17/1/7.
  * 申明persion的实体bean
  */
case class Person(@BeanProperty val name:String, @BeanProperty val age:Int){}

object SparkContextUtils {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val textRdd = sc.textFile("/Users/zhuhuihui/ruby-works/validate.rb")

    val arraySeq = Array(100,200,101,201,200)
    val arrayRdd = sc.parallelize(arraySeq)
    val listRdd = sc.makeRDD(List(100,500,300,400))

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

    arrayRdd.filter(x=>customFilter(x)).foreach(x=>println("filter="+x))

    println(arrayRdd.countByValue().foreach(e => {val (k,v) = e ;println("countByValue="+k+":"+v)}))//按值去重复

    arrayRdd.groupBy(x=>(x+10)).foreach(x=>println("groupBy="+x))

    arrayRdd.keyBy(x=>(x+10)).foreach(x=>println("keyBy="+x))

    println("reduce="+arrayRdd.reduce((a,b)=>a+b))//里面的元素相加

    println("textRdd..count ="+textRdd.count())
    println("textRdd..flatMap..count = "+textRdd.flatMap(line=>(line.split(" "))).count())
    val rk = textRdd.flatMap(line=>(line.split(" "))).map(world=>(world,1)).reduceByKey((a,b)=>a+b)
    rk.foreach(x=>println("flatMap,reduceByKey==="+x))



    //val tp_array = Array(100,200,101,201,200)
    //val tpRdd = sparkContext.parallelize(tp_array)
    //tpRdd.groupBy(x=>x+1).collect()

    val tp_array = Array(Tuple1(100,"spark"),Tuple1(100,"hadoop"),Tuple1(99,"hase"),Tuple1(98,"java"),Tuple1(98,"hive"))
    val tpRdd = sc.parallelize(tp_array)
    tpRdd.groupBy(_._1).collect()
    //tpRdd.groupByKey()

    val p = new Person("aaa",12)
    p.getName

    sc.stop();//停止sparkContext在driver

  }



  /**
   * 可以定义比较复杂的filter
   **/
  def customFilter(x:Int):Boolean={
     x>=100
  }


}
