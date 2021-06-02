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
 * created 2019/01/06.
 * 使用反射的方式把RDD转化为DataFrame
 */
public class RDD2DataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrame")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //读取本地文件创建RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\students.txt");
        //通过map算子处理数据，转换为Student类型的RDD
        JavaRDD<Student> students = lines.map(new Function<String, Student>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Student call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                Student stu = new Student();
                stu.setId(Integer.valueOf(lineSplited[0].trim()));
                stu.setName(lineSplited[1]);
                stu.setAge(Integer.valueOf(lineSplited[2].trim()));
                return stu;
            }
        });
        //创建SQLContext对象
        SQLContext sqlContext = new SQLContext(sc);

        //通过SQLContext调createDataFrame方法把RDD转化为DataFrame
        // 使用反射方式，将RDD转换为DataFrame
        // 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
        // 因为Student.class本身就是反射的一个应用
        // 然后底层还得通过对Student Class进行反射，来获取其中的field
        // 这里要求，JavaBean必须实现Serializable接口，是可序列化的
        DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);

        //拿到一个DataFrame后，将其注册为students的临时表，通过sql语句进行查询
        studentDF.registerTempTable("students");

        //针对临时表students，通过sql语句查询年龄小于18的数据,就是teenager
        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        //将DataFrame格式的数据转换为JavaRDD格式的数据
        JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        //因为转换过去的RDD的类型时Row,所以还得进行map映射为Student类型的RDD
        JavaRDD<Student> teenagerStudentsRDD = teenagerRDD.map(new Function<Row, Student>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Student call(Row row) throws Exception {
                //System.out.println(row);
                // row中的数据的顺序，可能是跟我们期望的是不一样的！
                int id = row.getInt(0);
                String name = row.getString(2);
                int age = row.getInt(1);
                Student stu = new Student(id,name,age);
                return stu;
            }
        });

        //将数据收集回来进行打印-----注意收集到driver端-----数据量太大会导致内存溢出
        List<Student> studentList = teenagerStudentsRDD.collect();

        //增强for循环遍历
        for(Student stu : studentList){
            System.out.println(stu);
        }

        //释放资源
        sc.close();
    }
}
