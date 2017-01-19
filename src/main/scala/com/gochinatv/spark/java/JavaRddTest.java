package com.gochinatv.spark.java;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
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
        scoreList.add(new Tuple2<String, Integer>("圆钢",99999));
        scoreList.add(new Tuple2<String, Integer>("张玮",98));
        scoreList.add(new Tuple2<String, Integer>("阿辉",20));
        scoreList.add(new Tuple2<String, Integer>("阿纲",29));
        scoreList.add(new Tuple2<String, Integer>("小徐",12));
        scoreList.add(new Tuple2<String, Integer>("小微",89));
        scoreList.add(new Tuple2<String, Integer>("小微1",89));


        List<Tuple2<String,Integer>> moneyList = new ArrayList<Tuple2<String, Integer>>();
        moneyList.add(new Tuple2<String, Integer>("圆钢",1989));
        moneyList.add(new Tuple2<String, Integer>("张玮",1654));
        moneyList.add(new Tuple2<String, Integer>("jack",20));
        moneyList.add(new Tuple2<String, Integer>("tomcat",29));
        moneyList.add(new Tuple2<String, Integer>("小徐",234));
        moneyList.add(new Tuple2<String, Integer>("小微",185));

        JavaRDD<Tuple2<String, Integer>> score_parallelize = sc.parallelize(scoreList);
        JavaRDD<Tuple2<String, Integer>> money_parallelize = sc.parallelize(moneyList);

        JavaPairRDD<String, Integer> pairRdd1 = score_parallelize.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2);
            }
        });

        JavaPairRDD<String, Integer> pairRdd2 = money_parallelize.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<String, Integer>(tuple._1, tuple._2);
            }
        });

        /** join:   注意这里只能打印关联的上的信息,必须两个rdd都村在才能打印出来*/
        /*JavaPairRDD<String, Tuple2<Integer, Integer>> join =pairRdd1.join(pairRdd2);
        join.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                int score = tuple._2._1;
                int money = tuple._2._2;
                System.out.println("姓名:"+tuple._1+" 分数:"+score+" money:"+money);
            }
        });*/
        System.out.println("===========================================================");

        /** leftOuterJoin:   注意这里打印出左边rdd的所有元素,右边关联上的打印,没关联上的不打印*/
        /** (姓名:阿辉 分数:20 money:0),(姓名:圆钢 分数:100 money:1989),(姓名:圆钢 分数:99999 money:1989)    */
        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftOutJoin = pairRdd1.leftOuterJoin(pairRdd2);
        leftOutJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> tuple) throws Exception {
                int score = tuple._2._1;
                int money = tuple._2._2.or(0);
                System.out.println("姓名:"+tuple._1+" 分数:"+score+" money:"+money);
            }
        });

        System.out.println("===========================================================");

        /** leftOuterJoin:   注意这里打印出右边rdd的所有元素,左边关联上的打印,没关联上的不打印*/
        /*JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightOutJoin = pairRdd1.rightOuterJoin(pairRdd2);
        rightOutJoin.foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>,Integer>>>() {
            public void call(Tuple2<String, Tuple2<Optional<Integer>,Integer>> tuple) throws Exception {
                int score = tuple._2._1.or(0);
                int money = tuple._2._2;
                System.out.println("姓名:"+tuple._1+" 分数:"+score+" money:"+money);
            }
        });*/



        /** cogroup :  当出现相同Key时, join会出现笛卡尔积, 而cogroup的处理方式不同*/
        /** (阿纲,([29],[])), (圆钢,([100, 99999],[1989]))  */
        JavaPairRDD<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> cogroup = pairRdd1.cogroup(pairRdd2);
        System.out.println(cogroup.collect().toString());
    }
}
