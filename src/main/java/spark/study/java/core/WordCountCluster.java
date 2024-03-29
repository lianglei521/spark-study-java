package spark.study.java.core;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/23.
 */

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
 * 将java开发的wordcount程序部署到spark集群上运行
 */
public class WordCountCluster {
    public static void main(String[] args) {
        // 如果要在spark集群上运行，需要修改的，只有两个地方
        // 第一，将SparkConf的setMaster()方法给删掉，默认它自己会去连接
        // 第二，我们针对的不是本地文件了，修改为hadoop hdfs上的真正的存储大数据的文件

        // 实际执行步骤：
        // 1、将spark.txt文件上传到hdfs上去
        // 2、使用我们最早在pom.xml里配置的maven插件，对spark工程进行打包
        // 3、将打包后的spark工程jar包，上传到机器上执行
        // 4、编写spark-submit脚本
        // 5、执行spark-submit脚本，提交spark应用到集群执行
        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");

        JavaSparkContext sc = new JavaSparkContext(conf);

        //写的是hdfs集群的url
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");


        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });


        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });


        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {

            private static final long seriaVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+"出现了"+wordCount._2+"次");
            }
        });

        sc.close();
    }
}
