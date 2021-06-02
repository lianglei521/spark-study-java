package spark.study.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/18.
 * 基于HDFS文件目录的WordCount的程序
 */
public class HDFSWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("HDFSWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //通过textFileStream方法读取HDFS文件目录，创建JavaDStream流
        //注意----这个程序必须提交到spark集群上去运行，否则不会有结果
        JavaDStream<String> lines = jssc.textFileStream("hdfs://spark1:9000/wordcount_dir");
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long seriaVersionUID =1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID  = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairDStream<String,Integer> wordcount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID =1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        wordcount.print();
        Thread.sleep(5000);

        jssc.start();
        jssc.awaitTermination();
        jssc.stop();

    }
}
