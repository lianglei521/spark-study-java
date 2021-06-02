package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/28.
 * 共享变量Broadcast
 */
public class BroadcastVariable {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("Broadcast")
                .setMaster("local");
        //创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);
        //创建外部变量
        final int factor =2;
        // 在java中，创建共享变量，就是调用SparkContext的broadcast()方法
        // 获取的返回结果是Broadcast<T>类型
        final Broadcast<Integer> factorBroadcast = sc.broadcast(factor);
        //并行化集合，创建初始化RDD
        List<Integer> numList = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbers = sc.parallelize(numList);
        //对RDD中的元素进行multiple操作
        JavaRDD<Integer> multipleNum = numbers.map(new Function<Integer, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(Integer v) throws Exception {
                //获取广播变量的值
                int factor = factorBroadcast.value();
                return v * factor;
            }
        });
        //打印结果
        multipleNum.foreach(new VoidFunction<Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public void call(Integer v) throws Exception {
                System.out.println(v);
            }
        });

        //释放资源
        sc.close();

    }
}
