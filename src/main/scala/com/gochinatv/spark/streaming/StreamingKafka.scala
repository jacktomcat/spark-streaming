package com.gochinatv.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * Created by zhuhuihui on 17/3/13.
  */
object StreamingKafka {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    //收到消息格式 (key237,{"id":"236","ts":"2017-03-24 17:34:29","count":"9","value":"39","agreeid":"323"})
    val inputStream = KafkaUtils.createStream(ssc,"localhost:2181","streaming-g01", Map[String, Int]("streaming-click" -> 1))
    val ds_value = inputStream.map(msg=>msg._2)

    //注意下面的写法是从DStream转换为 PairDStreamFunctions 可以使用高级的功能
    //Extra functions available on DStream of (key, value) pairs through an implicit conversion
    //val pairsDstream = inputStream.map(line=>(line._2.split(",")(0),line._2))


    //过滤消息中的数据filter
    val filter_value = ds_value.filter(msg=> {
      val raw = JSON.parseFull(msg)
      raw match {
        case Some(map: Map[String, String]) =>  {
          map.get("count").get.toInt>5
        }
      }
    })


    //filter_value.countByValueAndWindow()
    //filter_value.countByWindow()
    //filter_value.reduceByWindow()
    //filter_value.saveAsObjectFiles()
    //filter_value.saveAsTextFiles()
    //filter_value.transform()
    //filter_value.transformWith()
    //filter_value.union()
    //filter_value.window()

    //{"id":"0","ts":"2017-03-27 16:17:50","count":"9","value":"38","agreeid":"325"},{"id":"2","ts":"2017-03-27 16:17:50","count":"11","value":"41","agreeid":"832"}
    //reduce 合并
    /*val reduce_value = filter_value.reduce((a,b)=>{a.concat(",").concat(b)})
    reduce_value.print()*/


    //({"id":"236","ts":"2017-03-27 15:46:42","count":"6","value":"77","agreeid":"437"}, 1)
    //对DStream进行reduceByKey
    /*val count_value = filter_value.countByValue()
    count_value.print()*/

    // 1289 对输入dstream求count
    /*val count_value = filter_value.count()
    count_value.print()*/

    //把最后的结果打印出来DStream.foreachRDD =>转换为RDD
    /*filter_value.foreachRDD(rdd=>{
      rdd.collect().foreach(value=>{
        val raw = JSON.parseFull(value)
        raw match {
          case Some(map: Map[String, String]) =>  {
            println("id="+map.get("id").get+",ts="+map.get("ts").get+",agreeid="+map.get("agreeid").get)
          }
        }
      })
    })*/

    ssc.start()
    ssc.awaitTermination()
  }

}
