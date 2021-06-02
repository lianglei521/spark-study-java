package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/08.
 * Parquet数据源之自动推断分区
 */
public class ParquetPartitionDiscovery {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Parquet数据源之自动推断分区");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //本应该读取的是hdfs上的parquet文件来推断分区
        DataFrame userDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-study/users/gender=male/country=US/users.parquet");
        userDF.printSchema();
        userDF.show();
        sc.close();
    }
}
