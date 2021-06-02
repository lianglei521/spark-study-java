package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/08.
 * 通用的load和save----------注意读取保存的都是parquet文件
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        //创建SparkConf,SparkContext,SQLContext对象
        SparkConf conf = new SparkConf()
                .setAppName("GenericLoadSave")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //sqlContext读取本地文件创建dataFrame
        DataFrame data = sqlContext.read().load("C:\\Users\\Administrator\\Desktop\\临时文件\\users.parquet");
        data.select("name","favorite_color").write().save("C:\\Users\\Administrator\\Desktop\\临时文件\\users2.parquet");

        sc.close();
    }
}
