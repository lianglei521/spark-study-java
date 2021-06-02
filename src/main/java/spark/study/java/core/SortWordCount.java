package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/29.
 */
public class SortWordCount {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");

        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取本地文件创建初始化RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\hello.txt");

        //创建words RDD
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));//此处返回的是一个list
            }
        });

        //创建pair RDD
        JavaPairRDD<String,Integer> pair = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        //创建wordCount RDD
        JavaPairRDD<String,Integer> wordCount = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //wordCount RDD 反转---创建countWord RDD
        JavaPairRDD<Integer,String> countWord = wordCount.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2,t._1);
            }
        });

        //countWord RDD排序---降序
        JavaPairRDD<Integer,String> sortCountWord = countWord.sortByKey(false);

        //sortCountWord RDD反转---> sortWordCount RDD
        JavaPairRDD<String,Integer> sortWordCount = sortCountWord.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2,t._1);
            }
        });

        //打印输出
        sortWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        //释放资源
        sc.close();
    }
}
