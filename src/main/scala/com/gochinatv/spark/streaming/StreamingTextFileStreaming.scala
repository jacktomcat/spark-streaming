package com.gochinatv.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhuhuihui on 17/1/8.
  * 从文件目录中读取流数据
  */
object StreamingTextFileStreaming {


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("StreamingSocketText").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(10))

    //file,hdfs
    val textDstream = streamingContext.textFileStream("/Users/zhuhuihui/test")
    textDstream.flatMap(line=>line.split(" ")).map(world=>(world,1)).reduceByKey((a,b)=>a+b).print()

    streamingContext.start()
    streamingContext.awaitTermination()
    //streamingContext.awaitTerminationOrTimeout(2000)

  }

}
