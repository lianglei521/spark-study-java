package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/28.
 */
public class Persist {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("persist")
                .setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取本地文件，创建初始化RDD
        // cache()或者persist()的使用，是有规则的
        // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
        // 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
        // 而且，会报错，大量的文件会丢失
        JavaRDD<String> lines = sc.textFile(
                "C:\\Users\\Administrator\\Desktop\\临时文件\\新建日记本文档.txt")
                //.cache();//进行持久化---极大的提高的性能10倍
                .persist(StorageLevel.MEMORY_ONLY());

        long startTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        long cost = endTime -startTime;
        System.out.println("cost:"+cost+"MillisTimes");

        startTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        cost = endTime -startTime;
        System.out.println("cost:"+cost+"MillisTimes");

        sc.close();

    }
}
