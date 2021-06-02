package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 */
public class AggregateByKey {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("AggregateByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lineRDD = sc.textFile("C:\\Users\\Administrator\\Desktop\\HelloWord.java");
        JavaRDD<String> wordsRDD = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        JavaPairRDD<String,Integer> pairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        //使用aggregateByKey算子
        //第一个参数是根据每个key聚合时的初始值
        //第二个函数是相当于map-side端的本地聚合（Seq Function）
        //第三个函数是相当于reduce-side端的combine全局聚合（Combine Function）
        JavaPairRDD<String,Integer> wordCountRDD = pairRDD.aggregateByKey(
                0,
                new Function2<Integer, Integer, Integer>() {
                    private static final long seriaVersionUID = 1L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                new Function2<Integer, Integer, Integer>() {
                    private static final long seriaVersionUID = 1L;
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        for(Tuple2<String,Integer> tp : wordCountRDD.collect()){
            System.out.println(tp._1+":"+tp._2);
        }

        sc.close();
    }
}
