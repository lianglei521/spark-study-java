package spark.study.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/20.
 * 基于滑动窗口的热点搜索词实时统计
 */
public class WindowHotWord {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("基于热点搜索词实时统计")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //日志格式为： leo java
        JavaReceiverInputDStream<String> searchLog = jssc.socketTextStream("spark1",9999);
        //将searchLog转化为searchWord
        JavaDStream<String> searchWord = searchLog.map(new Function<String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public String call(String searchLog) throws Exception {
                return searchLog.split(" ")[1];
            }
        });
        //将searchWord转化为wordAndOne
        JavaPairDStream<String,Integer> wordAndOne = searchWord.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        //开始通过滑动窗口函数，执行聚合操作
        //第二个参数是滑动窗口的大小--->60秒一个窗口
        //第三个参数是滑动窗口的间隔时间----->每隔10秒去最近时间的一个窗口
        JavaPairDStream<String,Integer> hotWordByWindow = wordAndOne.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        },Durations.seconds(60),Durations.seconds(10));

        //通过transformToPair算子来对窗口里的RDD进行操作统计
        JavaPairDStream<String, Integer> wordCountDS = hotWordByWindow.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> wordCountRDD) throws Exception {
                //让RDD的key,value互换位置
                JavaPairRDD<Integer, String> countWordRDD = wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    private static final long seriaVersionUID = 1L;

                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> wordCount) throws Exception {
                        return new Tuple2<Integer, String>(wordCount._2, wordCount._1);
                    }
                });
                //根据key进行排序
                JavaPairRDD<Integer, String> sortedCountWord = countWordRDD.sortByKey(false);
                //再将RDD的key,value互换位置
                JavaPairRDD<String, Integer> sortedWordCount = sortedCountWord.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    private static final long seriaVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> sortedCountWord) throws Exception {
                        return new Tuple2<String, Integer>(sortedCountWord._2, sortedCountWord._1);
                    }
                });
                //收集前三个数据，并打印
                List<Tuple2<String, Integer>> top3WordCount = sortedWordCount.take(3);
                for (Tuple2<String, Integer> tp : top3WordCount) {
                    System.out.println(tp._1 + ":" + tp._2);
                }

                return wordCountRDD;
            }
        });
        //打印每个对应的窗口函数里的数据
        wordCountDS.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
