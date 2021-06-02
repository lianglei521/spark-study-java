package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 * 学生成绩查询案例
 */
public class MapPartitions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapPartitions")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> studentNames = Arrays.asList("张三","李四","王五","赵六");
        JavaRDD<String> studentNameRDD = sc.parallelize(studentNames);
        final Map<String,Double> studentScoreMap = new HashMap<String, Double>();
        studentScoreMap.put("张三",85.0);
        studentScoreMap.put("李四",86.0);
        studentScoreMap.put("王五",87.0);
        studentScoreMap.put("赵六",82.0);
        // mapPartitions
        // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
        // mapPartitions算子，一次处理一个partition中所有的数据

        // 推荐的使用场景
        // 如果你的RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
        // 但是如果你的RDD的数据量特别大，比如说10亿，不建议用mapPartitions，可能会内存溢出
        JavaRDD<Double> scoresRDD = studentNameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterable<Double> call(Iterator<String> it) throws Exception {
                // 因为算子一次处理一个partition的所有数据
                // call函数接收的参数，是iterator类型，代表了partition中所有数据的迭代器
                // 返回的是一个iterable类型，代表了返回多条记录，通常使用List类型
                List<Double> scores = new ArrayList<Double>();
                Double score;
                while(it.hasNext()){
                    String name = it.next();
                    score = studentScoreMap.get(name);
                    scores.add(score);
                }
                return scores;
            }
        });

        for (Double score : scoresRDD.collect()){
            System.out.println(score);
        }

        sc.close();
    }
}
