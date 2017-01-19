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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Created by zhuhh on 17/1/19.
 */
public class JavaRddTest {

    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("JavaRddTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> jackRdd = sc.textFile("hdfs://slave04:8020/user/admin/streaming/jack.txt");
        JavaRDD<String> nameRdd = sc.textFile("hdfs://slave04:8020/user/admin/streaming/name.txt");

        JavaRDD<String> javaFlatMapRDD = jackRdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.asList(split);
            }
        });

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

        /*****单词出现次数进行统计(1:[haoop,spark],2:[json,jackjboss])并降序 groupByKey,sortByKey*************************/
        /*
        JavaPairRDD<Integer, String> pairRdd3 = pairRdd2.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2, tuple._1);
            }
        });

        JavaPairRDD<Integer, Iterable<String>> pairRdd4 = pairRdd3.groupByKey().sortByKey(false);
        for(Tuple2<Integer, Iterable<String>> tpl: pairRdd4.collect()){
            System.out.println(tpl._2.toString()+"======"+tpl._1);
        }*/

        /**************************RDD合并过滤,union、filter************************/
        /*JavaRDD<String> newRdd = jackRdd.union(nameRdd);

        JavaRDD<String> afterRdd = newRdd.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return Arrays.asList(split);
            }
        });

        JavaRDD<String> filterRdd = afterRdd.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                return line.contains("jackjboss");
            }
        });*/

        /**************************RDD合并过滤,union、filter*********************************/
        List<Tuple2<String,Integer>> scoreList = new ArrayList<Tuple2<String, Integer>>();
        scoreList.add(new Tuple2<String, Integer>("圆钢",100));
        scoreList.add(new Tuple2<String, Integer>("张玮",98));
        scoreList.add(new Tuple2<String, Integer>("阿辉",20));
        scoreList.add(new Tuple2<String, Integer>("阿纲",29));
        scoreList.add(new Tuple2<String, Integer>("小徐",12));
        scoreList.add(new Tuple2<String, Integer>("小微",89));


        List<Tuple2<String,Integer>> namesList = new ArrayList<Tuple2<String, Integer>>();
        scoreList.add(new Tuple2<String, Integer>("圆钢",1989));
        scoreList.add(new Tuple2<String, Integer>("张玮",1654));
        //scoreList.add(new Tuple2<String, Integer>("阿辉",20));
        //scoreList.add(new Tuple2<String, Integer>("阿纲",29));
        scoreList.add(new Tuple2<String, Integer>("小徐",234));
        scoreList.add(new Tuple2<String, Integer>("小微",185));

        JavaRDD<Tuple2<String, Integer>> score_parallelize = sc.parallelize(scoreList);
        JavaRDD<Tuple2<String, Integer>> name_parallelize = sc.parallelize(namesList);

        JavaPairRDD<String, Integer> pairRdd1 = score_parallelize.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2);
            }
        });

        JavaPairRDD<String, Integer> pairRdd2 = name_parallelize.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2);
            }
        });

        JavaPairRDD<String, Tuple2<Integer, Integer>> join = pairRdd1.join(pairRdd2);

    }
}
