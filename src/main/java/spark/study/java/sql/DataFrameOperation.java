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
public class DataFrameOperation {
    public static void main(String[] args) {
//        //创建sparkconf对象
//        SparkConf conf = new SparkConf()
//                .setAppName("DataFrameCreate");
//        //创建JavaSparkContext对象
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        //创建SqlContext对象
//        SQLContext sqlContext = new SQLContext(sc);
//        //创建DataFrame对象
//        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/students.json");
//        //打印dataframe中所有的数据(select * from ....)
//        df.show();
//
//        //打印dataframe的元数据信息(schema)
//        df.printSchema();
//
//        //查询某列所有数据
//        df.select("name").show();
//
//        //查询某系列数据，并对列进行计算
//        df.select(df.col("name"),df.col("age").plus(1)).show();
//
//        //根据某一列的值进行过滤
//        df.filter(df.col("age").lt(18)).show();
//
//        //根据某一列分组，然后进行聚合
//        df.groupBy(df.col("age")).count().show();

        // 创建DataFrame
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 创建出来的DataFrame完全可以理解为一张表
        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/students.json");

        // 打印DataFrame中所有的数据（select * from ...）
        df.show();
        // 打印DataFrame的元数据（Schema）
        df.printSchema();
        // 查询某列所有的数据
        df.select("name").show();
        // 查询某几列所有的数据，并对列进行计算
        df.select(df.col("name"), df.col("age").plus(1)).show();
        // 根据某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        // 根据某一列进行分组，然后进行聚合
        df.groupBy(df.col("age")).count().show();

    }
}
