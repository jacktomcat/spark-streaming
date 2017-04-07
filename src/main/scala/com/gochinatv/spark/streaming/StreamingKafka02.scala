package com.gochinatv.spark.streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.util.parsing.json.JSON

/**
  * Created by tingyun on 2017/4/6.
  */
object StreamingKafka02 {

  def main(args: Array[String]): Unit = {

    val topic = "streaming-click"
    val topic_group = "streaming-g01"
    val sparkConf = new SparkConf().setAppName("StreamingKafka02").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //ssc.checkpoint("E:\\checkpoint")

    val sqlContext = new SQLContext(ssc.sparkContext)

    //消息格式 (key237,{"id":"236","ts":"2017-03-24 17:34:29","count":"9","value":"39","agreeId":"323"})
    val inputStream = KafkaUtils.createStream(ssc,"localhost:2181", topic_group, Map[String, Int](topic -> 1))
    val ds_value = inputStream.map(_._2).map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, String]])

    val distinctDStream = ds_value.filter(msg => msg.get("count").get.toInt>0).map(kv =>{
      val map = Map(
        "id" -> kv.get("id").get,
        "ts" -> kv.get("ts").get.substring(0,16),
        "count" -> kv.get("count").get.toInt,
        "value" -> kv.get("value").get.toInt,
        "agreeId" -> kv.get("agreeId").get)
      map
    }).transform(rdd=>{
      //"{\"id\":\"98\",\"ts\":\"2017-03-30 17:21:37\",\"count\":\"4\",\"value\":\"4\",\"agreeId\":\"2\"}"
      //"{\"id\":\"98\",\"ts\":\"2017-03-30 17:21:37\",\"count\":\"4\",\"value\":\"4\",\"agreeId\":\"2\"}"
      //"{\"id\":\"98\",\"ts\":\"2017-03-30 17:21:37\",\"count\":\"4\",\"value\":\"4\",\"agreeId\":\"2\"}"
      //"{\"id\":\"100\",\"ts\":\"2017-03-30 17:21:37\",\"count\":\"4\",\"value\":\"4\",\"agreeId\":\"2\"}"
      rdd.distinct() //这里rdd会按照上面的map去重操作输出出来结果为
      /*Map(count -> 4, ts -> 2017-03-30 17:21, agreeId -> 2, id -> 98, value -> 4)
      Map(count -> 4, ts -> 2017-03-30 17:21, agreeId -> 2, id -> 100, value -> 4)*/
    })

    /*val updateFunc = (currentVal: Seq[(Int,Int)], prevVal: Option[(Int,Int)]) =>{
        val prev = prevVal.getOrElse(0,0)
        var count = 0
        var value = 0
        currentVal.foreach(t =>{
          count = prev._1 + t._1
          value = prev._2 + t._2
        })
        Some(count,value)
    }
    val pairDstream = distinctDStream.map(map => (map.get("agreeId").get + "," + map.get("ts").get, (map.get("count").get.toString.toInt, map.get("value").get.toString.toInt))) //pairDstreamFunction
    val result = pairDstream.updateStateByKey(updateFunc)
    result.print()*/


    //updateStateByKey 所有批次的累加值 加上 window 函数试试？？？
    /*val counts = (current:Seq[Int],prev:Option[Int])=>{
      Option(current.sum + prev.getOrElse(0))
    }
    val countPairDstream = distinctDStream.map(v =>(v.get("agreeId").get.toString,v.get("count").get.toString.toInt))
    val countResult = countPairDstream.updateStateByKey(counts)
    countResult.print()*/

    //cogroup
    /*val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val double = countPairDstream.cogroup(countPairDstream)
    double.print()*/

    /**
      * fullOuterJoin 笛卡尔积 展示的方式 tuple(this),tuple(other)
      * (2017-04-07 17:49,(Some(3),Some(3)))
      * (2017-04-07 17:49,(Some(3),Some(5)))
      * (2017-04-07 17:49,(Some(5),Some(3)))
      * (2017-04-07 17:49,(Some(5),Some(5)))
      */
   /* val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val fullJoin = countPairDstream.fullOuterJoin(countPairDstream)
    fullJoin.print()*/

    /**
      * join 笛卡尔积 展示的方式 tuple(this,other)
      * (2017-04-07 17:58,(5,5))
      * (2017-04-07 17:58,(5,3))
      * (2017-04-07 17:58,(3,5))
      * (2017-04-07 17:58,(3,3))
      */
    /*val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val join = countPairDstream.join(countPairDstream)
    join.print()*/


    /**
      * leftOuterJoin 返回 tuple(this,tuple(other))
      * (2017-04-07 18:10,(4,Some(Map(count -> 5, ts -> 2017-04-07 18:10, agreeId -> 5, id -> 0, value -> 10))))
      */
    /*val otherDStream = ds_value.filter(msg => msg.get("count").get.toInt>2).map(kv =>{
      val map = Map(
        "id" -> kv.get("id").get,
        "ts" -> kv.get("ts").get.substring(0,16),
        "count" -> kv.get("count").get.toInt,
        "value" -> kv.get("value").get.toInt,
        "agreeId" -> kv.get("agreeId").get)
      map
    })
    val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val otherCountPairDstream = otherDStream.map(v =>(v.get("ts").get.toString,v))
    val leftOutJoin = countPairDstream.leftOuterJoin(otherCountPairDstream)
    leftOutJoin.print()*/


    /**
      * groupByKey  (key,arrayBuffer())
      * (2017-04-07 18:15,ArrayBuffer(2, 4, 1))
      */
   /* val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val groupByKey = countPairDstream.groupByKey()
    groupByKey.print()*/


   /**
     * reduceByKey
     * (2017-04-07 18:28,11)
    */
    val countPairDstream = distinctDStream.map(v =>(v.get("ts").get.toString,v.get("count").get.toString.toInt))
    val reduceByKey = countPairDstream.reduceByKey((a,b)=>a+b)
    reduceByKey.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
