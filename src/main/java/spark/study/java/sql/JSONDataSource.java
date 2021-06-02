package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/09.
 * JSON数据源
 */
public class JSONDataSource {
    public static void main(String[] args) {
        //创建SparkConf ,SparkContext,SQLContext对象
        SparkConf conf = new SparkConf()
                .setAppName("JSON数据源")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //读取本地JSON文件，创建DataFrame
        DataFrame studentsNameAndScoreDF = sqlContext.read().format("json").load("C:\\Users\\Administrator\\Desktop\\临时文件\\students.json");
        //接下来就是注册临时表
        studentsNameAndScoreDF.registerTempTable("studentsScore");
        //执行sql语句查询分数大于80分的学生
        DataFrame goodStudentNameAndScoreDF = sqlContext.sql("select name,score from studentsScore where score >= 80");
        //转化为JavaRDD进行transformation操作----获取每个好学生的name----为后面的查询提供支持
        List<String> goodStudentNames = goodStudentNameAndScoreDF.javaRDD().map(new Function<Row, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        //测试代码
       // System.out.println(goodStudentNames);

        //创建studentInfoJSONs
        List<String> studentNameAndAgeJSONs = new ArrayList<String>();
        //易错点----json串中出现了中文符号逗号
        studentNameAndAgeJSONs.add("{\"name\":\"Leo\",\"age\":18}");
        studentNameAndAgeJSONs.add("{\"name\":\"Marry\",\"age\":17}");
        studentNameAndAgeJSONs.add("{\"name\":\"Jack\",\"age\":19}");

//        studentNameAndAgeJSONs.add("{\"name\":\"Leo\", \"age\":18}");
//        studentNameAndAgeJSONs.add("{\"name\":\"Marry\", \"age\":17}");
//        studentNameAndAgeJSONs.add("{\"name\":\"Jack\", \"age\":19}");

        JavaRDD<String> studentNameAndAgeJSONRDD = sc.parallelize(studentNameAndAgeJSONs, 1);
        //通过读取JSONRDD创建DataFrame
        DataFrame studentNameAndAgeJSONsDF = sqlContext.read().json(studentNameAndAgeJSONRDD);

        //测试代码
        studentNameAndAgeJSONsDF.show();

        //注册临时表，执行sql语句进行查询好学生的基本信息
        studentNameAndAgeJSONsDF.registerTempTable("student_info");
        String sql = "select name,age from student_info where name in (";
        //遍历好学生的name,拼接完整sql语句
        for (int i =0;i<goodStudentNames.size();i++){
            sql += "'" + goodStudentNames.get(i) + "'";
            if(i < goodStudentNames.size()-1){
                sql += ",";
            }
        }
        sql += ")";
        //测试代码
       // String sql = "select name ,age from student_info where name = Leo";


        DataFrame goodStudentNameAndAgeDF = sqlContext.sql(sql);

        //把两个DataFrame转化为javaRDD,再transformations操作变为JavaPairRDD----最后join
        JavaPairRDD<String, Tuple2<Integer, Integer>> studentNameAndScoreAndAge = goodStudentNameAndScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                //易错点---获取score时用的getLong方法
                return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(
                goodStudentNameAndAgeDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
                    private static final long seriaVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0), Integer.valueOf(String.valueOf(row.getLong(1))));
                    }
                })
        );

        //把JavaPairRDD变成JavaRDD<Row>
        JavaRDD<Row> goodStudentNameAndAgeAndScoreRowRDD = studentNameAndScoreAndAge.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._2, t._2._1);
            }
        });

        //创建Schema信息
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType nameAndAgeAndScoreStructType = DataTypes.createStructType(structFields);

        //根据JavaRDD<Row>与StructType创建DataFRame数据集
        DataFrame goodStudentNameAndAgeAndScoreDF = sqlContext.createDataFrame(goodStudentNameAndAgeAndScoreRowRDD, nameAndAgeAndScoreStructType);

        //将好学生的信息保存到json文件中
        goodStudentNameAndAgeAndScoreDF.write().format("json").save("C:\\Users\\Administrator\\Desktop\\临时文件\\good_student.json");

        //释放资源
        sc.close();

    }
}
