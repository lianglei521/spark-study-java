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
 * 求两个RDD的交集
 */
public class Intersection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Intersection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> projectMember1List = Arrays.asList("张三","李四","王五","赵六","磊磊");
        List<String> projectMember2List = Arrays.asList("秦王妃","琼华公主","晧澜","公主雅","磊磊");
        JavaRDD<String> projectMember1RDD = sc.parallelize(projectMember1List,2);
        JavaRDD<String> projectMember2RDD = sc.parallelize(projectMember2List,2);
        //求两个RDD的交集
        JavaRDD<String> publicMemberRDD = projectMember1RDD.intersection(projectMember2RDD);
        for(String publicMember:publicMemberRDD.collect()){
            System.out.println(publicMember);
        }
        sc.close();
    }
}
