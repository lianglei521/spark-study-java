package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * author:liangsir
 * qq:714628767
 * created 2018/12/25.
 * 使用本地文件创建RDD
 * 统计文本文件字数
 */
public class LocalFile {
    public static void main(String[] args) {
        //创建SparkConf对象
        SparkConf conf = new SparkConf()
                .setAppName("统计文本文件字数")
                .setMaster("local");

        //创建SaprkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        //使用SparkContext及其子类的textFile()方法，对本地文件创建RDD
        JavaRDD<String> lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\新建日记本文档.jnt");

        //计算每一行的字节数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(String line) throws Exception {
                return line.length();
            }
        });

        //计算所有行的字数
        Integer count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //输出文本的统计字数
        System.out.println("文本的字数为："+count);

        sc.close();

    }
}
