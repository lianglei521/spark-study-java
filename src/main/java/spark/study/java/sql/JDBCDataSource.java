package spark.study.java.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/11.
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        //创建SparkConf ,SparkContext, SQLContext对象
        SparkConf conf = new SparkConf()
                .setAppName("JDBCDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //分别将两张mysql中的表用sqlContext的read+format+options配置+load方法加载为DataFrame数据集
        Map<String,String> options = new HashMap<String,String>();
        options.put("url","jdbc:mysql://spark1:3306/testdb");
        options.put("dbtable","student_infos");
        DataFrame student_info_df = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable","student_scores");
        DataFrame student_score_df = sqlContext.read().format("jdbc").options(options).load();
        //将DataFrame转化为javaRDD<Row>再转化为javaPairRDD<String,Integer,Integer>z最后进行join操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> student_info_score = student_info_df.javaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    private static final long seriaVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
                    }
                }
        ).join(student_score_df.javaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    private static final long seriaVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
                    }
                }
        ));

        //将javaPairRDD转化为javaRDD<Row>
        JavaRDD<Row> student_info_score_row = student_info_score.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1,t._2._1,t._2._2);
            }
        });

        //过滤分数大于80分的数据
        JavaRDD<Row> good_student = student_info_score_row.filter(new Function<Row, Boolean>() {
            private static final long seriaVersionUID =1L;
            @Override
            public Boolean call(Row row) throws Exception {
                if(row.getInt(2)>80){
                    return true;
                }
                return false;
            }
        });
        //将javaRDD<Row>z转化为dataFrame
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame good_student_df = sqlContext.createDataFrame(good_student,structType);

        //收集为Row[] 并打印
        Row[] good_student_rows = good_student_df.collect();
        for (Row good_student_row : good_student_rows){
            System.out.println(good_student_row);
        }

        //最后将dataFrame转化为javaRDD<Row>,遍历存储到mysql,redis,hbase中
        good_student_df.javaRDD().foreachPartition(new VoidFunction<Iterator<Row>>() {
            @Override
            public void call(Iterator<Row> rowIterator) throws Exception {
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement stmt = null;
                String sql = null;
                try {
                    conn = DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb","","");
                    stmt = conn.createStatement();
                    while(rowIterator.hasNext()){
                        Row row = rowIterator.next();
                        System.out.println(row);
                        sql = "insert into good_student_infos values ("+"'"+row.getString(0)+"',"+row.get(1)+","+row.getInt(2)+")";
                        stmt.executeUpdate(sql);
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }finally {
                    if(conn != null){
                        try {
                            conn.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                    if(stmt !=null){
                        try {
                            stmt.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }

            }
        });

        //释放资源
        sc.close();
    }
}
