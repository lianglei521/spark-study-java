package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 * RDD的合并算子
 */
public class Union {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Union")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> partment1List = Arrays.asList("张三","李四","王五","赵六");
        List<String> partment2List = Arrays.asList("秦王妃","琼华公主","晧澜","公主雅");
        JavaRDD<String> partment1RDD = sc.parallelize(partment1List,2);
        JavaRDD<String> partment2RDD = sc.parallelize(partment2List,2);
        //合并两个RDD的算子
        JavaRDD<String> partmentRDD = partment1RDD.union(partment2RDD);
        for(String name: partmentRDD.collect()){
            System.out.println(name);
        }
        sc.close();
    }
}
