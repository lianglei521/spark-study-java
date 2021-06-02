package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/06.
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        //创建sparkconf对象
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建SqlContext对象
        SQLContext sqlContext = new SQLContext(sc);
        //创建DataFrame对象
        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/students.json");
        //打印df中所有的数据
        df.show();
    }
}
