package com.gochinatv.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhuhuihui on 17/3/13.
  */
object StreamingKafka {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    //KafkaUtils.createStream()
    //KafkaUtils.createDirectStream()


  }

}
