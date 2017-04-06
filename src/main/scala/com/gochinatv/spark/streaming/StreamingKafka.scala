package com.gochinatv.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.parsing.json.JSON

/**
  * Created by zhuhuihui on 17/3/13.
  *
  * batchTime = 60s
  * window = 5*60s
  * slidewindow = 2*60
  *
  * 根据agreeid和ts截取到分进行分组聚合, count, value求sum
  */
case class Message(id:String,ts:String,count:Int,value:Int, agreeId:String)

object StreamingKafka {

  def main(args: Array[String]) {
    val topic = "streaming-click"
    val topic_group = "streaming-g01"
    val sparkConf = new SparkConf().setAppName("StreamingKafka").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    //ssc.checkpoint("E:\\checkpoint")

    val sqlContext = new SQLContext(ssc.sparkContext)

    //消息格式 (key237,{"id":"236","ts":"2017-03-24 17:34:29","count":"9","value":"39","agreeId":"323"})
    val inputStream = KafkaUtils.createStream(ssc,"localhost:2181", topic_group, Map[String, Int](topic -> 1))
    val ds_value = inputStream.map(_._2).map(JSON.parseFull(_)).map(_.get.asInstanceOf[scala.collection.immutable.Map[String, String]])
    val msg_data =
      ds_value.map(msg=>Message(
        msg.get("id").get,
        msg.get("ts").get.substring(0,16),
        msg.get("count").get.toInt,
        msg.get("value").get.toInt,
        msg.get("agreeId").get
      ))

    import sqlContext.implicits._

    // 重新查询在前，window函数在后，这样不会把之前的记录累加上去 transform
    /*val transformRdd = msg_data.transform(rdd=>
      {
        rdd.toDF().registerTempTable("click_data")
        val df = sqlContext.sql("select agreeId,ts,sum(count),sum(value) from click_data group by agreeId,ts")
        df.rdd
      })
    val result = transformRdd.window(Seconds(60),Seconds(30))
    result.print()*/

    // window函数在前，重新查询在后，这样不会把之前的记录累加上去
   /* val transformRdd = msg_data.window(Seconds(60),Seconds(30)).transform(
      rdd=>
      {
        rdd.toDF().registerTempTable("click_data")
        val df = sqlContext.sql("select agreeId,ts,sum(count),sum(value) from click_data group by agreeId,ts ")
        df.rdd
      }
    )
    transformRdd.print()*/


    //过滤消息中的数据 filter,countByWindow
    /*val filter_value = ds_value.filter(msg=> {
      msg.get("count").get.toInt>2
    })
    val countByWindow =  filter_value.countByWindow(Seconds(30),Seconds(10))
    countByWindow.print()*/


    //reduceByWindow 和 reduce差不多，只是多了一个时间窗口,这里一共发了10个消息
    //console output -> Message(0-1-2-3-4-5-6-7-8-9,2017-04-05,28,44,3-2-2-2-2-3-3-3-1-1)
    /*val reduceByWindow = msg_data.reduceByWindow((msg1,msg2)=>{
      val id = msg1.id.concat("-").concat(msg2.id)
      val count = msg1.count + msg2.count
      val value = msg1.value + msg2.value
      val agreeId = msg1.agreeId.concat("-").concat(msg2.agreeId)
      val msg = new Message(id, "2017-04-05", count, value, agreeId)
      msg
    },Seconds(30),Seconds(10))
    reduceByWindow.print()*/

    //保存数据序列化文件至hadoop
    //msg_data.saveAsObjectFiles("/hdfs/clickstream","dat")

    //保存数据文本格式文件至hadoop
    //msg_data.saveAsTextFiles("/hdfs/clickstream","txt")


    //countByValueAndWindow 需要hdfs支持
    /*val filter_value = ds_value.filter(msg=> {
      msg.get("count").get.toInt>2
    })
    val valueAndWindow = filter_value.countByValueAndWindow(Seconds(30),Seconds(10))
    valueAndWindow.print()*/

    /*
     transformWith 转换RDD操作，但是需要另一个DStream
    Map(count -> 1, ts -> 2017-04-06 14:18:24, agreeId -> 1, id -> 8, value -> 7)
    Map(count -> 4, ts -> 2017-04-06 14:18:23, agreeId -> 1, id -> 3, value -> 6)
    Map(count -> 1, ts -> 2017-04-06 14:18:24, agreeId -> 1, id -> 7, value -> 9)
    Map(count -> 3, ts -> 2017-04-06 14:18:22, agreeId -> 1, id -> 0, value -> 5)
    Map(count -> 1, ts -> 2017-04-06 14:18:23, agreeId -> 1, id -> 4, value -> 9)
    Map(count -> 4, ts -> 2017-04-06 14:18:24, agreeId -> 1, id -> 9, value -> 6)
    Map(count -> 5, ts -> 2017-04-06 14:18:23, agreeId -> 1, id -> 5, value -> 3)
    Map(count -> 5, ts -> 2017-04-06 14:18:23, agreeId -> 1, id -> 2, value -> 9)

    val this_val = ds_value.filter(msg=> {
      msg.get("count").get.toInt>2
    })

    val other_val = ds_value.filter(msg=>{
      msg.get("count").get.toInt<2
    })

    val transformWith = this_val.transformWith(other_val,(thisRdd: RDD[Map[String,String]],otherRdd: RDD[Map[String,String]])=>
      thisRdd.union(otherRdd).distinct()
    )
    transformWith.print()*/


    //slice  Some(0)==============Some(2017-04-06 15:58:40)
    /*val this_val = ds_value.filter(msg=> {
      msg.get("count").get.toInt>2
    })
    this_val.print()

    this_val.foreachRDD(rdd=>{
      this_val.slice(new Time(System.currentTimeMillis-10000),new Time(System.currentTimeMillis)).flatMap(_.collect()).foreach(map=>{
        println(map.get("id")+"=============="+map.get("ts"))
      })
    })*/


    //注意下面的写法是从DStream转换为 PairDStreamFunctions 可以使用高级的功能
    //Extra functions available on DStream of (key, value) pairs through an implicit conversion
    //val pairsDstream = inputStream.map(line=>(line._2.split(",")(0),line._2))


    //{"id":"0","ts":"2017-03-27 16:17:50","count":"9","value":"38","agreeId":"325"},{"id":"2","ts":"2017-03-27 16:17:50","count":"11","value":"41","agreeId":"832"}
    //reduce 合并
    /*val reduce_value = filter_value.reduce((a,b)=>{a.concat(",").concat(b)})
    reduce_value.print()*/


    //({"id":"236","ts":"2017-03-27 15:46:42","count":"6","value":"77","agreeId":"437"}, 1)
    //对DStream进行 countByValue
    /*val filter_value = ds_value.filter(msg=> {
      msg.get("count").get.toInt>2
    })
    val count_value = filter_value.countByValue()
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
            println("id="+map.get("id").get+",ts="+map.get("ts").get+",agreeId="+map.get("agreeId").get)
          }
        }
      })
    })*/

    ssc.start()
    ssc.awaitTermination()
  }

}
