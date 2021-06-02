package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/30.
 * 分组Top3
 */
public class GroupTop3 {
    public static void main(String[] args) {
        //创建SparkConf 对象
        SparkConf conf = new SparkConf()
                .setAppName("分组Top3")
                .setMaster("local");

        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取本地文件创建初始化JavaRDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\临时文件\\score.txt");

        //创建pairs RDD 如："class1 8"---->(class1,8)
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String[] splitedLine = line.split(" ");
                return new Tuple2<String, Integer>(splitedLine[0],Integer.valueOf(splitedLine[1]));
            }
        });

        //按照班级分组
        JavaPairRDD<String,Iterable<Integer>> groupedScores = pairs.groupByKey();

        //取每组中的前三
        JavaPairRDD<String,Iterable<Integer>> groupedTop3 = groupedScores.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                //创建数组容器装Top的分数
                Integer[] top3 = new Integer[3];
                //取班级
                String className = t._1;
                //取班级对应的所有分数
                Iterator<Integer> classScores = t._2().iterator();
                //通过以下算法来取迭代器中的前三个元素
                while(classScores.hasNext()){
                    Integer score = classScores.next();
                    for(int i =0;i<3;i++){
                        if(top3[i] == null){
                            top3[i] = score;
                            break;
                            //注意每次都是从头比较---所以元素一直都是从大到小的排列
                        }else if(score > top3[i]){
                            //从此位置开始所有元素后移，然后score顶替此位置元素
                            for(int j =2;j>i;j--){
                                top3[j] = top3[j-1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }
                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        //遍历打印groupedTop3
        groupedTop3.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1);
                Iterator<Integer> scoresTop3 = t._2().iterator();
                while(scoresTop3.hasNext()){
                    Integer score = scoresTop3.next();
                    System.out.println(score);
                }
                System.out.println("==================================");
            }
        });
        //释放资源
        sc.close();
    }
}
