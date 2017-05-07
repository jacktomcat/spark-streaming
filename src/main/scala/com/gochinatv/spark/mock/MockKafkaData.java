package com.gochinatv.spark.mock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MockKafkaData {
   
	public static  AtomicInteger global_id = new AtomicInteger(0);
	public static int ERROR = 0;
	
	/**
	 * 构建数据格式{id:1,ts:2017-03-24 12:32:34,count:10,value:200,agreeid:190}
	 * topic:[app,server,sys,network]
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.2.150:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("max.block.ms", 1000); //配置超时时间
		//props.put("request.timeout.ms", 1000);
		props.put("metadata.fetch.timeout.ms", 2000);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		
		int i=1;
		
		while(true){
		  Thread.sleep(200L);
		  
		  MockKafkaData mtd = new MockKafkaData();
		  String data = "{\"id\":\""+global_id.getAndIncrement()+"\",\"ts\":\""+mtd.getTimeNow()+"\",\"count\":\""+getCount()+"\",\"value\":\""+getValue()+"\",\"agreeId\":\""+getAgreeId()+"\"}";
		  
		  producer.send(new ProducerRecord<String, String>("streaming-click", "key"+i, data),new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(null == metadata){
					try {
						ERROR ++;
						System.out.println(exception.getClass().toString());
						return;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				ERROR = 0;
			}
		  });
		  if(ERROR>0){
			  break;
		  }
		  System.out.println(data);
		  i++;
		  if(i==10000)break;
		}
		
		producer.flush();
		producer.close();
	}
	
	public static int getAgreeId(){
		int random=(int) (Math.random()*10+1);
		return random;
	}
	
	public static int getCount(){
		int random=(int) (Math.random()*5+1);
		return random;
	}
	
	public static int getValue(){
		int random=(int) (Math.random()*10+1);
		return random;
	}
	
	public String getTimeNow(){
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sdf.format(date);
	}
	
}
