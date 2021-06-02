package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/02.
 */
public class TakeSample {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TakeSample")
                .setMaster("local");
        JavaSparkContext sc= new JavaSparkContext(conf);
        List<String> staffList = Arrays.asList("张三","李四","王五","赵六","琼华","公主雅","皓澜","秦王妃","若曦","兰妃");
        JavaRDD<String> staffRDD = sc.parallelize(staffList);

        // takeSample算子
        // 与sample不同之处，两点
        // 1、action操作---看返回值就知道，sample是transformation操作
        // 2、不能指定抽取比例，只能是抽取几个
        //false表示抽出的元素不往回放
        List<String> luckyStaff = staffRDD.takeSample(false, 2);
        for (String staff: luckyStaff){
            System.out.println(staff);
        }
        sc.close();
    }
}
