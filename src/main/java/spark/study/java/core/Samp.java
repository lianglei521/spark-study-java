package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/01.
 * 年会抽奖案例
 */
public class Samp {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Samp")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> staffs = Arrays.asList("张三","李四","王五","赵六","小倩","晧澜","吕不韦","琼华公主","雅儿","若曦");
        JavaRDD<String> staffRDD = sc.parallelize(staffs,2);
        //samp算子，随机抽取----transformation算子
        //第一个参数，第二个参数表示抽取0.1的人数，第三个参数表示种子（不建议）
        //withReplacement：表示抽出样本后是否在放回去，true表示会放回去，这也就意味着抽出的样本可能有重复
        //fraction ：抽出多少，这是一个double类型的参数,0-1之间，eg:0.3表示抽出30%
         //seed：表示一个种子，根据这个seed随机抽取，一般情况下只用前两个参数就可以，那么这个参数是干嘛的呢，这个参数一般用于调试，有时候不知道是程序出问题还是数据出了问题，就可以将这个参数设置为定值
        JavaRDD<String> luckyStaffRDD = staffRDD.sample(false, 0.1);
        for (String luckyStaff: luckyStaffRDD.collect()){
            System.out.println(luckyStaff);
        }

        sc.close();
    }
}
