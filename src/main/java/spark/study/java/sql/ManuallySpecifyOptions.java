package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/08.
 * 手动指定数据源-----这种方式牛逼！！！
 */
public class ManuallySpecifyOptions {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("手动指定数据源")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //format方法的使用----格式化---以哪种格式读取，以哪种格式存储
        DataFrame peopleDF = sqlContext.read().format("json").load("C:\\Users\\Administrator\\Desktop\\临时文件\\people.json");
        peopleDF.select("name","age").write().format("parquet").save("C:\\Users\\Administrator\\Desktop\\临时文件\\people.parquet");
        sc.close();
    }
}
