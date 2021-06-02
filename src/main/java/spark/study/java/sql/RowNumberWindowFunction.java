package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/12.
 * row_number()开窗函数实战
 */
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        //创建SparkConf,JavaSparkContext,HiveContext对象
        SparkConf conf = new SparkConf()
                .setAppName(" row_number()开窗函数实战");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //注意HiveContext里面传递的是SparkContext对象
        HiveContext hiveContext = new HiveContext(sc.sc());

        //删除sales表，并创建，再导入数据
        hiveContext.sql("drop table if exists sales");
        hiveContext.sql("create table if not exists sales (product string,category string,revenue bigint)");
        hiveContext.sql("load data local inpath '/usr/local/spark-study/resources/sales.txt'into table sales");

        //对表中的数据进行查询--按category分组----计算每组的top3的sale
        DataFrame top3SaleDF = hiveContext.sql("select product,category,revenue "
                + "from "
                + "(select product,category,revenue,row_number() over (partition by category order by revenue desc) rank from sales) temp_sales "
                + "where rank <=3"
        );

        //保存到top3_sales表中------先判断存在不，存在就删除
        hiveContext.sql("drop table if exists top3_sales");
        top3SaleDF.saveAsTable("top3_sales");

        //释放资源
        sc.close();

    }
}
