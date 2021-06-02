package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/08.
 * Parquet数据源之使用编程加载数据
 */
public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Parquet数据源之使用编程加载数据")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //一般json ,parquet,文件都用sqlContext的read方法+format指定格式+load方法来读取-----返回的是Dataframe数据集----然后可以sql查询了
        //不过这里是转化为JavaRDD---进行的SparkCore的处理
        DataFrame userDF = sqlContext.read().format("parquet").load("C:\\Users\\Administrator\\Desktop\\临时文件\\users.parquet");
        //注册临时表
        userDF.registerTempTable("users");
        //sql语句进行复杂查询
        DataFrame userNameDF = sqlContext.sql("select name from user");
            //DataFrame--->JavaRDD<Row>----->map处理
            JavaRDD<String> names = userNameDF.javaRDD().map(new Function<Row, String>() {
                private static final long seriaVersionUID = 1L;
                @Override
                public String call(Row row) throws Exception {
                    return "name:"+row.getString(0);
                }
        });

        List<String> nameList = names.collect();
        for (String name: nameList){
            System.out.println(name);
        }

        sc.close();
    }
}
