package spark.study.java.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/20.
 * 基于transform的实时广告计费日志黑名单过滤
 */
public class TransformBlackList {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("基于transform的实时广告计费日志黑名单过滤")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        //先造一份黑名单数据----转化为RDD
        //第一个参数表示用户，第二个参数表示是否启用黑名单，true表示启用
        List<Tuple2<String,Boolean>> blackList = new ArrayList<Tuple2<String,Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("tom",true));
        final JavaPairRDD<String, Boolean> userBlackListRDD = jssc.sc().parallelizePairs(blackList);

        //实时读取userADsClickLog数据，(date + user) 创建流数据DStream
        JavaReceiverInputDStream<String> ADsClickLogDS = jssc.socketTextStream("spark1",9999);
        //转化为pairDStream
        JavaPairDStream<String,String> userADsClickLogDS = ADsClickLogDS.mapToPair(new PairFunction<String, String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                return new Tuple2<String, String>(log.split(" ")[1],log);
            }
        });

        //进行transform转化------最后返回的是合法的用户日志
        JavaDStream<String> userValidADsClickLogDS = userADsClickLogDS.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userADsClickLogRDD) throws Exception {
                //让用户点击日志与用户黑名单leftOutjoin，保证没有join到的黑名单用户的日志也不会丢失
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userADsClickLogRDD.leftOuterJoin(userBlackListRDD);
                //然后过滤掉有黑名单用户的数据
                JavaPairRDD<String,Tuple2<String,Optional<Boolean>>> filterRDD = joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    private static final long seriaVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tp) throws Exception {
                        //如果join到了黑名单用户，并且该黑名单用户启用了，那就过滤掉
                        if(tp._2._2.isPresent() && tp._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });
                //进行transformation算子操作，返回需要的格式---String
                JavaRDD<String> validADsClickLog = filterRDD.map(new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {
                    private static final long seriaVersionUID = 1L;
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tp) throws Exception {
                        return tp._2._1();
                    }
                });
                return validADsClickLog;
            }
        });
        //打印数据
        userValidADsClickLogDS.print();

        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
