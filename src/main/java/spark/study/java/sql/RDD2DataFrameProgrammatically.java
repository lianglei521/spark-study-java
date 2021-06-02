package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/07.
 */
public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        //创建SparkConf,SparkContext,SQLContext对象
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrame")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //读取本地文件创建初始化JavaRDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\students.txt");

        // 分析一下
        // 它报了一个，不能直接从String转换为Integer的一个类型转换的错误
        // 就说明什么，说明有个数据，给定义成了String类型，结果使用的时候，要用Integer类型来使用
        // 而且，错误报在sql相关的代码中
        // 所以，基本可以断定，就是说，在sql中，用到age<=18的语法，所以就强行就将age转换为Integer来使用
        // 但是，肯定是之前有些步骤，将age定义为了String
        // 所以就往前找，就找到了这里
        // 往Row中塞数据的时候，要注意，什么格式的数据，就用什么格式转换一下，再塞进去
        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                //这步容易忘----RowFactory的使用
                return RowFactory.create(Integer.valueOf(lineSplited[0]),lineSplited[1],Integer.valueOf(lineSplited[2]));
            }
        });

        // 第二步，编程方式动态构造元数据
        // 比如说，id、name等，field的名称和类型，可能都是在程序运行过程中，动态从mysql db里
        // 或者是配置文件中，加载出来的，是不固定的
        // 所以特别适合用这种编程的方式，来构造元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);

        //第三步构建DataFrame---类型时Row
        DataFrame studentDF = sqlContext.createDataFrame(studentRDD,structType);

        //后面就可以使用DataFrame了
        //注册临时表
        studentDF.registerTempTable("students");

        //sql查询
        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        //DataFrame转化为RDD
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        //收集
        List<Row> teenagerRow = teenagerRDD.collect();

        //遍历打印------row就是一组数据
        for(Row row:teenagerRow){
            System.out.println(row);
        }

        //释放资源
        sc.close();
    }
}
