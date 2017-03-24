package com.gochinatv.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    val flatmap = inputStream.flatMap(msg=> (msg._2,msg._1))
    //ds_value.

    //inputStream.print()
    //KafkaUtils.createDirectStream()

    ssc.start()
    ssc.awaitTermination()
  }

}
