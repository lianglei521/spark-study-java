package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/26.
 */
public class TransFormationOperation {
    public static void main(String[] args) {
        //map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        //join();
        cogroup();

    }


    /**
     * map算子案例，将集合中每一个元素乘以2
     */
    public static void map() {
        //创建SparkConf对象
        SparkConf conf  = new SparkConf()
                .setAppName("map算子案例，将集合中每一个元素乘以2")
                .setMaster("local");

        //创建JavaSaprkContext的对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //构造集合
        List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);

        //并行化集合，创建初始化RDD
        JavaRDD<Integer> numRDD =  sc.parallelize(nums);

        // 使用map算子，将集合中的每个元素都乘以2
        // map算子，是对任何类型的RDD，都可以调用的
        // 在java中，map算子接收的参数是Function对象
        // 创建的Function对象，一定会让你设置第二个泛型参数，这个泛型类型，就是返回的新元素的类型
        // 同时call()方法的返回类型，也必须与第二个泛型类型同步
        // 在call()方法内部，就可以对原始RDD中的每一个元素进行各种处理和计算，并返回一个新的元素
        // 所有新的元素就会组成一个新的RDD
        JavaRDD<Integer> newNumRDD = numRDD.map(new Function<Integer, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(Integer num) throws Exception {
                return num * 2;
            }
        });

        //遍历打印
        newNumRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer newNum) throws Exception {
                System.out.println(newNum);
            }
        });

        sc.close();
    }

    /**
     * filter算子案例，过滤集合中的偶数
     */
    private static void filter() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("filter")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 模拟集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        //对初始RDD执行filter算子，过滤出其中的偶数
        // filter算子，传入的也是Function，其他的使用注意点，实际上和map是一样的
        // 但是，唯一的不同，就是call()方法的返回类型是Boolean
        // 每一个初始RDD中的元素，都会传入call()方法，此时你可以执行各种自定义的计算逻辑
        // 来判断这个元素是否是你想要的
        // 如果你想在新的RDD中保留这个元素，那么就返回true；否则，不想保留这个元素，返回false
        JavaRDD<Integer> filterRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Boolean call(Integer v) throws Exception {
                return v % 2 == 0;
            }
        });

        //遍历打印
        filterRDD.foreach(new VoidFunction<Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });

        sc.close();
    }

    /**
     * flatMap案例：将文本行拆分为多个单词
     */
    private static void flatMap() {
        // 创建SparkConf
        SparkConf conf = new SparkConf()
                .setAppName("flatMap")
                .setMaster("local");
        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构造集合
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");

        // 并行化集合，创建RDD
        JavaRDD<String> lines = sc.parallelize(lineList);

        // 对RDD执行flatMap算子，将每一行文本，拆分为多个单词
        // flatMap算子，在java中，接收的参数是FlatMapFunction
        // 我们需要自己定义FlatMapFunction的第二个泛型类型，即，代表了返回的新元素的类型
        // call()方法，返回的类型，不是U，而是Iterable<U>，这里的U也与第二个泛型类型相同
        // flatMap其实就是，接收原始RDD中的每个元素，并进行各种逻辑的计算和处理，返回可以返回多个元素
        // 多个元素，即封装在Iterable集合中，可以使用ArrayList等集合
        // 新的RDD中，即封装了所有的新元素；也就是说，新的RDD的大小一定是 >= 原始RDD的大小
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long seriaVersion = 1L;

            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        // 打印新的RDD
        words.foreach(new VoidFunction<String>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * groupByKey算子案例，按照班级对成绩进行分组
     */
    public static void groupByKey(){
        //创建sparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("按照班级对成绩进行分组")
                .setMaster("local");
        //创建SparkContext的对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //构造键值对集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 85),
                new Tuple2<String, Integer>("class1", 89),
                new Tuple2<String, Integer>("class2", 83),
                new Tuple2<String, Integer>("class3", 87),
                new Tuple2<String, Integer>("class3", 81)
        );
        //并行化集合，创建javaPairRDD
        JavaPairRDD<String, Integer> score = sc.parallelizePairs(scoreList);
        //针对scoreRDD 执行groupByKey算子-----这个直接调用的算子
        // 针对scores RDD，执行groupByKey算子，对每个班级的成绩进行分组
        // groupByKey算子，返回的还是JavaPairRDD
        // 但是，JavaPairRDD的第一个泛型类型不变，第二个泛型类型变成Iterable这种集合类型
        // 也就是说，按照了key进行分组，那么每个key可能都会有多个value，此时多个value聚合成了Iterable
        // 那么接下来，我们是不是就可以通过groupedScores这种JavaPairRDD，很方便地处理某个分组内的数据
        JavaPairRDD<String, Iterable<Integer>> groupedScores = score.groupByKey();
        //打印groupedScores RDD
        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Iterable<Integer>> groupedScore) throws Exception {
                System.out.println("class:"+groupedScore._1);
                Iterator<Integer> ite = groupedScore._2.iterator();
                while(ite.hasNext()){
                    System.out.println(ite.next());
                }
                System.out.println("===============================");
            }
        });
        sc.close();
    }

    /**
     * 使用reduceByKey算子，按班级分组聚合成绩
     */
    public static void reduceByKey(){
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("按班级分组聚合成绩")
                .setMaster("local");
        //创建SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //并行化集合，创建初始化RDD
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 85),
                new Tuple2<String, Integer>("class2", 86),
                new Tuple2<String, Integer>("class3", 81),
                new Tuple2<String, Integer>("class1", 89),
                new Tuple2<String, Integer>("class2", 82)
        );
        JavaPairRDD<String, Integer> pairScore = sc.parallelizePairs(scoreList);
        //执行reduceByKey算子
        JavaPairRDD<String, Integer> reduceScore = pairScore.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //打印结果
        reduceScore.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        sc.close();
    }

    /**
     * sortByKey算子的使用，按照学生分数进行排序
     */
    public static void sortByKey(){
        //创建SparkConf 对象
        SparkConf conf = new SparkConf()
                .setAppName("按照学生分数进行排序")
                .setMaster("local");
        //创建SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //并行化集合，创建初始化RDD
        List<Tuple2<Double, String>> scoreList = Arrays.asList(
                new Tuple2<Double, String>(85.5, "leo"),
                new Tuple2<Double, String>(86.5, "lucy"),
                new Tuple2<Double, String>(83.2, "tom"),
                new Tuple2<Double, String>(89.4, "jerry"),
                new Tuple2<Double, String>(81.3, "lili"),
                new Tuple2<Double, String>(82.5, "joy")
        );

       //并行化集合，创建初始化RDD
        JavaPairRDD<Double, String> scorePairRDD = sc.parallelizePairs(scoreList);

        // 对scores RDD执行sortByKey算子
        // sortByKey其实就是根据key进行排序，可以手动指定升序，或者降序
        // 返回的，还是JavaPairRDD，其中的元素内容，都是和原始的RDD一模一样的
        // 但是就是RDD中的元素的顺序，不同了
        JavaPairRDD<Double, String> sortRDD = scorePairRDD.sortByKey(false);

        //打印结果
        sortRDD.foreach(new VoidFunction<Tuple2<Double, String>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<Double, String> t) throws Exception {
                System.out.println(t._1+"------"+t._2);
            }
        });
        sc.close();
    }

    /**
     * join案例，把学生和成绩关联起来
     */
    public static void join(){
        //创建SparkConf 对象
        SparkConf conf = new SparkConf()
                .setAppName("把学生和成绩关联起来")
                .setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 85),
                new Tuple2<Integer, Integer>(2, 86),
                new Tuple2<Integer, Integer>(3, 82),
                new Tuple2<Integer, Integer>(4, 88),
                new Tuple2<Integer, Integer>(5, 81)
                );
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "lucy"),
                new Tuple2<Integer, String>(3, "tom"),
                new Tuple2<Integer, String>(4, "jerry"),
                new Tuple2<Integer, String>(5, "joy")
        );

        //并行化集合，创建初始化RDD
        JavaPairRDD<Integer, String> stuRDD = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoRDD = sc.parallelizePairs(scoreList);

        //使用join算子
        JavaPairRDD<Integer, Tuple2<String, Integer>> stuAndScore = stuRDD.join(scoRDD);

        //打印结果
        stuAndScore.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("编号："+t._1+", 学生："+t._2._1+", 成绩："+t._2._2);
            }
        });
        //释放资源
        sc.close();
    }

    /**
     * cogroup案例，打印学生成绩
     */
    public static void cogroup(){
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("cogroup案例，打印学生成绩")
                .setMaster("local");
        //创建SparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //模拟集合
        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom")
        );
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 82),
                new Tuple2<Integer, Integer>(2, 83),
                new Tuple2<Integer, Integer>(3, 84),
                new Tuple2<Integer, Integer>(1, 85),
                new Tuple2<Integer, Integer>(2, 86),
                new Tuple2<Integer, Integer>(3, 87)
        );
        //并行化集合，创建初始化RDD
        JavaPairRDD<Integer, String> stuRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoRDD = sc.parallelizePairs(scores);

        //使用cogroup算子
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> stuAndScore = stuRDD.cogroup(scoRDD);

        //打印结果
        stuAndScore.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1+":"+t._2._1+":"+t._2._2);
            }
        });
        sc.close();
    }

}
