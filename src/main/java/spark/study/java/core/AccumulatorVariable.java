package spark.study.java.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/28.
 */
public class AccumulatorVariable {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("Accumulator")
                .setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建共享变量Accumulator,sc调用accumulator()方法
       final Accumulator<Integer> sum = sc.accumulator(0);
        //并行化集合，创建初始化RDD
        List<Integer> numList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numList);
        //遍历并累加
        numbers.foreach(new VoidFunction<Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Integer v) throws Exception {
                // 然后在函数内部，就可以对Accumulator变量，调用add()方法，累加值
                sum.add(v);
            }
        });
        // 在driver程序中，可以调用Accumulator的value()方法，获取其值
        System.out.println(sum.value());
        //释放资源
        sc.close();
    }
}
