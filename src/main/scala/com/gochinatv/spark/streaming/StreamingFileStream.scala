package com.gochinatv.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhuhuihui on 17/1/8.
  */
object StreamingFileStream {



  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingFileStream").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))
    val inputDstream =
      streamingContext.fileStream(
        "hdfs://slave04:8020/tmp/logs/admin/logs/application_1483271882538_0001",pa=>pa.getName.indexOf("sql")>0,true)
    println(inputDstream.count().toString)



    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
