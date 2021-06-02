package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 * 去重算子
 */
public class Distinct {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Distinct")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //构建网站的用户日志logs
        List<String> usersLog = Arrays.asList(
                "user1 2019-02-01 12:15:12",
                "user1 2019-02-02 12:15:12",
                "user1 2019-02-02 12:18:12",
                "user2 2019-02-03 18:15:12",
                "user2 2019-02-03 19:15:16",
                "user2 2019-02-04 20:19:12",
                "user3 2019-02-05 12:20:12",
                "user3 2019-02-05 13:15:12",
                "user3 2019-02-09 19:15:12"
        );
        JavaRDD<String> usersLogRDD = sc.parallelize(usersLog,2);
        JavaRDD<String> usersRDD = usersLogRDD.map(new Function<String, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public String call(String log) throws Exception {
                return log.split(" ")[0];
            }
        });
        //进行去重
        JavaRDD<String> distinctUserRDD = usersRDD.distinct();
        for (String user:distinctUserRDD.collect()){
            System.out.println(user);
        }

        sc.close();
    }
}
