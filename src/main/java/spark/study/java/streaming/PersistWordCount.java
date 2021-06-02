package spark.study.java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Media.print;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/19.
 * 基于UpdateStateByKey实现了checkpoint缓存机制的实时WordCount程序
 */
public class PersistWordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("PersistWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 第一点，如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，开启checkpoint机制
        // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
        // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在
        // 内存数据丢失的时候，可以从checkpoint中恢复数据

        // 开启checkpoint机制，很简单，只要调用jssc的checkpoint()方法，设置一个hdfs目录即可
        //注意---------->checkpoiont目录一定要新建，否则会出错
        jssc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");
        //jssc.checkpoint("./wordcount_checkpoint");

        // 然后先实现基础的wordcount逻辑
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark1",9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        // 到了这里，就不一样了，之前的话，是不是直接就是pairs.reduceByKey
        // 然后，就可以得到每个时间段的batch对应的RDD，计算出来的单词计数
        // 然后，可以打印出那个时间段的单词计数
        // 但是，有个问题，你如果要统计每个单词的全局的计数呢？
        // 就是说，统计出来，从程序启动开始，到现在为止，一个单词出现的次数，那么就之前的方式就不好实现
        // 就必须基于redis这种缓存，或者是mysql这种db，来实现累加

        // 但是，我们的updateStateByKey，就可以实现直接通过Spark维护一份每个单词的全局的统计次数
        JavaPairDStream<String,Integer> wordCountDS = pairs.updateStateByKey(
                // 这里的Optional，相当于Scala中的样例类，就是Option，可以这么理解
                // 它代表了一个值的存在状态，可能存在，也可能不存在
                new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            private static final long seriaVersionUID = 1L;
            // 这里两个参数
            // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
            // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
            // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                // 首先定义一个全局的单词计数
                Integer new_state = 0;
                // 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
                // 如果存在，说明这个key之前已经统计过全局的次数了
                if(state.isPresent()){
                    new_state = state.get();
                }
                // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
                // 次数
                for(Integer value : values){
                    new_state += value;
                }
                return Optional.of(new_state);
            }
        });

       /*//每次得到当前所有单词的统计次数之后，将其写入mysql存储，进行持久化，以便后续的J2EE应用程序显示
        wordCountDS.foreachRDD(new Function<JavaPairRDD<String, Integer>,Void>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountRDD) throws Exception {
                wordCountRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    private static final long seriaVersionUID = 1L;
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCountIterator) throws Exception {
                        //从连接池里获取一个连接
                        Connection conn = ConnectionPool.getConnection();
                        Statement statement = conn.createStatement();
                        while (wordCountIterator.hasNext()) {
                            Tuple2<String, Integer> tp =  wordCountIterator.next();
                            //String sql = "insert into wordcount (word,count) values ('"+tp._1+"'，"+tp._2+")";

                            String sql = "insert into wordcount(word,count) "
                                    + "values('" + tp._1 + "'," + tp._2 + ")";
                            statement.executeUpdate(sql);
                        }
                        //归还连接
                        ConnectionPool.returnConnection(conn);
                    }
                });
            return null;
            }
        });*/


        wordCountDS.foreachRDD(new Function<JavaPairRDD<String,Integer>, Void>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                // 调用RDD的foreachPartition()方法
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Integer>>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        // 给每个partition，获取一个连接
                        Connection conn = ConnectionPool.getConnection();

                        // 遍历partition中的数据，使用一个连接，插入数据库
                        Tuple2<String, Integer> wordCount = null;
                        while(wordCounts.hasNext()) {
                            wordCount = wordCounts.next();
                            System.out.println(wordCount._1+":"+wordCount._2);

                            String sql = "insert into wordcount(word,count) "
                                    + "values('" + wordCount._1 + "'," + wordCount._2 + ")";

                            Statement stmt = conn.createStatement();
                            stmt.executeUpdate(sql);
                        }

                        // 用完以后，将连接还回去
                        ConnectionPool.returnConnection(conn);
                    }
                });

                return null;
            }

        });
        //wordCountDS.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
