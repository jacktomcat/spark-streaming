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
    val inputStream = KafkaUtils.createStream(ssc,"192.168.2.150:2181","spark-test-01", Map[String, Int]("spark-test" -> 1))
    inputStream.print()
    //KafkaUtils.createDirectStream()

    ssc.start()
    ssc.awaitTermination()
  }

}
