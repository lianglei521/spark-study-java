package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/29.
 * Top3
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\top.txt",1);
        JavaPairRDD<Integer,String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(String line) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(line),line);
            }
        });
        JavaPairRDD<Integer,String> sortedPair = pairs.sortByKey(false);
        JavaRDD<Integer> sortedLine = sortedPair.map(new Function<Tuple2<Integer,String>, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });
        List<Integer> top3 = sortedLine.take(3);//take是action算子，返回类型时List
        for (Integer number: top3){
            System.out.println(number);
        }
        sc.close();
    }
}
