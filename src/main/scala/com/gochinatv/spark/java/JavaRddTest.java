package com.gochinatv.spark.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;


/**
 * Created by zhuhh on 17/1/19.
 */
public class JavaRddTest {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaRddTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> javaRdd = sc.textFile("hdfs://slave04:8020/user/admin/streaming/jack.txt");

        JavaRDD<String> javaFlatMapRDD = javaRdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.asList(split);
            }
        });

        /*****************************单词出现的次数(zhuhh,12)  reduceByKey、mapToPair**************************************/
        /*JavaPairRDD<String, Integer> pairRdd = javaFlatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String world) throws Exception {
                return new Tuple2<String, Integer>(world, 1);
            }
        });

        JavaPairRDD<String, Integer> pairRdd2 = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        for(Tuple2<String,Integer> tpl : pairRdd2.collect()){
            System.out.println(tpl._1+"======"+tpl._2);
        }*/

        /*****************************单词出现的次数排名 sortByKey、reduceByKey、mapToPair **************************************/
        /*
        JavaPairRDD<String, Integer> pairRdd = javaFlatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String world) throws Exception {
                return new Tuple2<String, Integer>(world, 1);
            }
        });

        JavaPairRDD<String, Integer> pairRdd2 = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        JavaPairRDD<Integer, String> pairRdd3 = pairRdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        }).sortByKey(false);

        JavaPairRDD<String, Integer> pairRdd4 = pairRdd3.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._2, tuple._1);
            }
        });

        for(Tuple2<String,Integer> tpl : pairRdd4.collect()){
            System.out.println(tpl._1+"======"+tpl._2);
        }
        */

        /*****************************打印总单词数  reduce***********************************/
        /*
        JavaRDD<Integer> mapRdd = javaFlatMapRDD.map(new Function<String, Integer>() {
            public Integer call(String word) throws Exception {
                return 1;
            }
        });

        Integer reduce = mapRdd.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(reduce);
        */



    }
}
