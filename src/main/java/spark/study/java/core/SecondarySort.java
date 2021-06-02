package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/29.
 * 二次排序
 * 1、实现自定义的key，要实现Ordered接口和Serializable接口，在key中实现自己对多个列的排序算法
 * 2、将包含文本的RDD，映射成key为自定义key，value为文本的JavaPairRDD
 * 3、使用sortByKey算子按照自定义的key进行排序
 * 4、再次映射，剔除自定义的key，只保留文本行
 */
public class SecondarySort {
    public static void main(String[] args) {
        //创建SparkConf 对象
        SparkConf conf = new SparkConf()
                .setAppName("自定义key二次排序")
                .setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取本地文件创建初始化RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\secondarySort.txt");
        //构建自定义key的pairRDD
        JavaPairRDD<SecondarySortKey,String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] splited = line.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.valueOf(splited[0]),Integer.valueOf(splited[1]));
                return new Tuple2<SecondarySortKey, String>(key,line);
            }
        });
        //根据自定义的key进行排序
        JavaPairRDD<SecondarySortKey,String> sortedPair = pairs.sortByKey(false);
        //剔除自定义的key
        JavaRDD<String> sortedLine = sortedPair.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public String call(Tuple2<SecondarySortKey, String> t) throws Exception {
                return t._2;
            }
        });
        //遍历打印输出
        sortedLine.foreach(new VoidFunction<String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(String line) throws Exception {
                System.out.println(line);
            }
        });
        //释放资源
        sc.close();
    }
}
