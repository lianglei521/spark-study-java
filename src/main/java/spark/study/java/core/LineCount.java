package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/26.
 * 统计每一行出现的次数
 */
public class LineCount {
    public static void main(String[] args) {
        //创建SparkConf 的对象
        SparkConf conf = new SparkConf()
                .setAppName("统计每一行出现的次数")
                .setMaster("local");
        //创建SparkContext的对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //本地文件创建RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\hello.txt");
        //创建pairRDD 的
        JavaPairRDD<String, Integer> pair = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<String, Integer>(line, 1);
            }
        });
        //创建lineCountRDD
        JavaPairRDD<String, Integer> lineCount = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //遍历打印
        lineCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> lineCount) throws Exception {
                System.out.println(lineCount._1+"出现了："+lineCount._2+"次");
            }
        });

        //释放资源
        sc.close();

    }
}
