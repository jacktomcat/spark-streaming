package com.gochinatv.spark.streaming

import org.apache.spark.SparkConf
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
    ssc.checkpoint("E:\\checkpoint")

    val sqlContext = new SQLContext(ssc.sparkContext)

    //消息格式 (key237,{"id":"236","ts":"2017-03-24 17:34:29","count":"9","value":"39","agreeId":"323"})
    val inputStream = KafkaUtils.createStream(ssc,"localhost:2181", topic_group, Map[String, Int](topic -> 1))
    val ds_value = inputStream.map(_._2).map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, String]])

    val distinctDStream = ds_value.filter(msg => msg.get("count").get.toInt>2).map(kv =>{
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

    /*val updateFunc = (currentVal: Seq[Map[String,Any]], prevVal: Option[Map[String,Any]]) =>{
        currentVal.toList.foreach(data => {
          val prev = prevVal.getOrElse(Map("count" -> "0","value" -> "0"))
          val count = data.get("count").get.toString.toInt + prev.get("count").get.toString.toInt
          val value = data.get("value").get.toString.toInt + prev.get("value").get.toString.toInt
          val result = Map(
            "agreeId" -> data.get("agreeId").get.toString,
            "ts" -> data.get("ts").get.toString,
            "count" -> count,
            "value" -> value
          )
          Option(result)
        })
    }*/
    val pairDstream = distinctDStream.map(map => (map.get("agreeId").get + "," + map.get("ts").get , map)) //pairDstreamFunction
    val result =
      pairDstream.update


    //updateStateByKey 所有批次的累加值 加上 window 函数试试？？？
    /*val counts = (current:Seq[Int],prev:Option[Int])=>{
      Option(current.sum + prev.getOrElse(0))
    }
    val countPairDstream = distinctDStream.map(v =>(v.get("agreeId").get.toString,v.get("count").get.toString.toInt))
    val countResult = countPairDstream.updateStateByKey(counts)
    countResult.print()*/

    ssc.start()
    ssc.awaitTermination()
  }

}
