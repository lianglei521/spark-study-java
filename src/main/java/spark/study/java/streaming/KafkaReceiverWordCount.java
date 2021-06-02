package spark.study.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/19.
 * 基于Kafka的Receiver方式的实时WordCount程序
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        //创建SparkConf,JavaStreamingContext对象
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("基于Kafka的Receiver方式的实时WordCount程序");
        //每5秒收集一次数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //使用KafkaUtils.createStream()方法，创建基于Kafka的数据输入流
        Map<String,Integer> topicConsumerThread = new HashMap<String, Integer>();
        topicConsumerThread.put("WordCount",1);//因为创建topic时给的分区是一个，所以在程序中相应的指定kafka的读取数据的线程数为1
        JavaPairReceiverInputDStream<String,String> index_lines = KafkaUtils.createStream(
                jssc,
                "192.168.33.11:2181,192.168.33.12:2181,192.168.33.13:2181",//zookeeper参数
                "DefaultConsumerGroup",
                topicConsumerThread
        );

        //开始WordCount程序的处理
        JavaDStream<String> words = index_lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterable<String> call(Tuple2<String, String> tp) throws Exception {
                return Arrays.asList(tp._2.split(" "));
            }
        });

        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairDStream<String,Integer> wordCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID =1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCount.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();


    }
}
