package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/27.
 */
public class ActionOperation {
    public static void main(String[] args) {
        //reduce();
        //collect();
        //count();
        //take();
        //saveAsTextFile();
        countByKey();
    }

    /**
     * reduce算子案例，求和1~5
     */
    private static void reduce(){
        //创建SparkConf 和JavaSparkContext对象
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        //并行化集合，创建初始化RDD
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        // 使用reduce操作对集合中的数字进行累加
        // reduce操作的原理：
        // 首先将第一个和第二个元素，传入call()方法，进行计算，会获取一个结果，比如1 + 2 = 3
        // 接着将该结果与下一个元素传入call()方法，进行计算，比如3 + 3 = 6
        // 以此类推
        // 所以reduce操作的本质，就是聚合，将多个元素聚合成一个元素
        Integer sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("1到5的和为："+sum);
        //关闭JavaSparkContext
        sc.close();
    }

    /**
     * collect算子案例，对每个数乘以2
     */
    private static void collect(){
        SparkConf conf = new SparkConf()
                .setAppName("reduce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        JavaRDD<Integer> multiNums = numbers.map(new Function<Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v) throws Exception {
                return v * 2;
            }
        });
        // 不用foreach action操作，在远程集群上遍历rdd中的元素
        // 而使用collect操作，将分布在远程集群上的doubleNumbers RDD的数据拉取到本地
        // 这种方式，一般不建议使用，因为如果rdd中的数据量比较大的话，比如超过1万条
        // 那么性能会比较差，因为要从远程走大量的网络传输，将数据获取到本地
        // 此外，除了性能差，还可能在rdd中数据量特别大的情况下，发生oom异常，内存溢出
        // 因此，通常，还是推荐使用foreach action操作，来对最终的rdd元素进行处理
        List<Integer> doubleNumberList = multiNums.collect();
        for(Integer num:doubleNumberList){
            System.out.println(num);
        }
        sc.close();
    }

    /**
     * count算子案例，统计有多少个元素
     */
    private static void count(){
        SparkConf conf = new SparkConf()
                .setAppName("count")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        long count = numbers.count();
        System.out.println("一共有"+count+"个元素");
        sc.close();
    }

    /**
     * take算子案例，取前几个元素(从远程上拉取到本地)
     */
    private static void take(){
        SparkConf conf = new SparkConf()
                .setAppName("take")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        List<Integer> top3Numbers = numbers.take(3);
        for (Integer num: top3Numbers){
            System.out.println(num);
        }
        sc.close();
    }

    /**
     * saveAsTextFile案例
     */
    private static void saveAsTextFile(){
        SparkConf conf = new SparkConf()
                .setAppName("savaAsTextFile");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numberList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        // 直接将rdd中的数据，保存在HFDS文件中
        // 但是要注意，我们这里只能指定文件夹，也就是目录
        // 那么实际上，会保存为目录中的/number.txt/part-00000文件
        numbers.saveAsTextFile("hdfs://spark1:9000/number.txt");
        sc.close();
    }

    /**
     * countByKey算子案例
     */
    private static void countByKey(){
        SparkConf conf = new SparkConf()
                .setAppName("countByKey")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String,String>> studentsList = Arrays.asList(
                new Tuple2<String,String>("class1","leo"),
                new Tuple2<String,String>("class2","tom"),
                new Tuple2<String,String>("class1","jack"),
                new Tuple2<String,String>("class2","lucy"),
                new Tuple2<String,String>("class1","jerry")
        );
        JavaPairRDD<String, String> students = sc.parallelizePairs(studentsList);
        Map<String,Object> countStus = students.countByKey();
        for (Map.Entry<String,Object> countStu: countStus.entrySet()){
            System.out.println(countStu.getKey()+"==="+countStu.getValue());
        }
        sc.close();
    }
}
