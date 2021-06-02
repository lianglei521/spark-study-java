package spark.study.java.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/19.
 * 基于kafka的direct方式的实时WordCount程序
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) {
        //创建SparkConf,JavaStreamingContext对象
        SparkConf conf = new SparkConf()
                .setAppName("基于kafka的direct方式的实时WordCount程序")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //这种方式有三个优点:
        //一是简化了并行读取的方式，receiver那种方式要创建多个DStream并行读取数据,而direct是创建RDD的多个partition和Kafka的partioton一一对应
        //二是在数据高可用时（数据零丢失），提高了性能，receiver是WALs写磁盘，多复制一份数据，很消耗性能，direct是通过kafka自身得高可靠机制，有备份
        //三是一次仅一次的事务机制，receiver是zokeeper记录offset与spark可能不同步造成数据多次重复消费，direct是spark自己追踪offset到checkpoint中，自己肯定是同步的

        //先造一份kafka参数
        Map<String,String> kafkaParams = new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list","spark1:9092,spark2:9092,spark3:9092");

        //然后创建一个set,里面放你要读取的topic，这个就很好了，可以并行读取多个topic
        Set<String> topics = new HashSet<String>();
        topics.add("WordCount");

        //创建DStream
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        //开始写wordcount程序
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
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
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCount.print();



        //让程序启动
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
