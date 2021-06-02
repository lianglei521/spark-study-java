package spark.study.java.core;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/25.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 * 计算1~10的值
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        //创建sparkconf对象
        SparkConf conf = new SparkConf()
                .setAppName("计算1~10的值")
                .setMaster("local");

        //创建JAvaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构建集合对象
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //并行化集合创建RDD对象
        JavaRDD<Integer> numRDD = sc.parallelize(nums);

        //对RDD进行计算
        Integer sum = numRDD.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        //打印结果
        System.out.println("1到10的和为："+sum);

        sc.close();

    }
}
