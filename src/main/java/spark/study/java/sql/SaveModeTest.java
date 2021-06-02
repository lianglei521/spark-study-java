package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/08.
 * 保存时指定模式---追加---覆盖---报错----忽略
 */
public class SaveModeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("保存时指定模式---追加---覆盖---报错----忽略")
                .set("spark.default.parallelism", "10")//这个是设置分区---就是并行度
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame peopleDF = sqlContext.read().format("json").load("C:\\Users\\Administrator\\Desktop\\临时文件\\people.json");
        //指定mode的存储语法是在参数里指定格式的
        peopleDF.select("name").save("C:\\Users\\Administrator\\Desktop\\临时文件\\people2.json","json",SaveMode.Append);
        sc.close();
    }
}
