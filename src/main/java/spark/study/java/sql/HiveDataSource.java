package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/10.
 * HIvedata数据源
 */
public class HiveDataSource {
    public static void main(String[] args) {
        //创建SparkConf ,JavaSparkContext,HiveContext对象
        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //注意，创建HIveContext时使用的SparkContext,所以要转化一下-------!!!!!!
        HiveContext hiveContext = new HiveContext(sc.sc());

        //如果hive中student_infos这张表已存在那么就先删除
        //注意为什么可以直接操作hive数据库中的表，这是因为spark1中的conf中导入了hive的hive-site.xml以及mysql-connection.jar
        hiveContext.sql("DROP TABLE IF EXISTS student_infos");
        //然后判断有没有student_infos这张表，没有就创建
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_infos (name STRING, age INT)");
        //导入student_infos.txt的数据到hive表中
        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH '/usr/local/spark-study/resources/student_infos.txt' "
                + "INTO TABLE student_infos");

        //如果hive中student_scores这张表已经存在那么就先删除
        hiveContext.sql("DROP TABLE IF EXISTS student_scores");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_scores (name STRING, score INT)");
        hiveContext.sql("LOAD DATA "
                + "LOCAL INPATH '/usr/local/spark-study/resources/student_scores.txt' "
                + "INTO TABLE student_scores");

        //两张表join查询分数大于80的学生的基本信息
        DataFrame goodStudentsDF = hiveContext.sql("SELECT si.name, si.age, ss.score "
                + "FROM student_infos si "
                + "JOIN student_scores ss ON si.name=ss.name "
                + "WHERE ss.score>=80");

        //将查询出来的数据保存到good_student_infos表中
        hiveContext.sql("drop table if exists good_student_infos");
        //以下这行不能有，否则会和saveAsTable冲突----报错
       // hiveContext.sql("create table if not exists good_student_infos (name string,age int,score int)");
        goodStudentsDF.saveAsTable("good_student_infos");

        //hiveContext可以通过table创建DataFrame
        DataFrame good_student_infosDF = hiveContext.table("good_student_infos");
        Row[] good_student_info_row = good_student_infosDF.collect();
        for (Row row:good_student_info_row){
            System.out.println(row);
        }

        //释放资源
        sc.close();
    }
}
