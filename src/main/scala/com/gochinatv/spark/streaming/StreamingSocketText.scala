package com.gochinatv.spark.streaming

/**
  * Created by zhuhuihui on 17/1/2.
  */
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds


/**
  * 从socket中取得流
  */
object StreamingSocketText {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingSocketText").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    val receiverInputDStream = streamingContext.socketTextStream("localhost", 9999)
    val words = receiverInputDStream.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.print()


    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
