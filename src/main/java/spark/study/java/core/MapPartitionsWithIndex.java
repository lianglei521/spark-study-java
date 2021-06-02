package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 * 查询带有班级学生成绩
 */
public class MapPartitionsWithIndex {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("MapPartitionsWithIndex")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> studentsName = Arrays.asList("张三","李四","王五","赵六");
        //两个并行度，相当于两个分区，也就是两个班级
        JavaRDD<String> studentsNameRDD = sc.parallelize(studentsName,2);
        final Map<String,Integer> studentScores = new HashMap<String,Integer>();
        studentScores.put("张三",78);
        studentScores.put("李四",88);
        studentScores.put("王五",89);
        studentScores.put("赵六",75);
        JavaRDD<String> scoreAndClassRDD = studentsNameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterator<String> call(Integer index, Iterator<String> it) throws Exception {
                List<String> scoreAndClass = new ArrayList<String>();
                Integer score = 0;
                while(it.hasNext()){
                    String name = it.next();
                    scoreAndClass.add(studentScores.get(name)+"_"+(index+1));
                }
                return scoreAndClass.iterator();
            }
        },true);  //true表示保留分区

        for (String scoreByClass: scoreAndClassRDD.collect()){
            System.out.println(scoreByClass);
        }
        sc.close();
    }
}
