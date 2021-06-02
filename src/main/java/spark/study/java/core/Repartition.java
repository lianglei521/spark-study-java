package spark.study.java.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/02.
 */
public class Repartition {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Coalesce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> staffList = Arrays.asList("张三","李四","王五","赵六","琼华","公主雅","皓澜","秦王妃","若曦","兰妃");
        JavaRDD<String> staffRDD = sc.parallelize(staffList,2);

        //一定要记住mapPartitionsWithIndex这个算子有两个参数
        JavaRDD<String> staffWithDepartment_2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterator<String> call(Integer department, Iterator<String> it) throws Exception {
                List<String> new_staffs = new ArrayList<String>();
                while(it.hasNext()){
                    String new_staff = it.next();
                    new_staffs.add("【部门"+(department+1)+"】："+new_staff);
                }
                return new_staffs.iterator();
            }
        },true);

        for(String staff: staffWithDepartment_2.collect()){
            System.out.println(staff);
        }

        // repartition算子，用于任意将rdd的partition增多，或者减少
        // 与coalesce不同之处在于，coalesce仅仅能将rdd的partition变少
        // 但是repartition可以将rdd的partiton变多

        // 建议使用的场景
        // 一个很经典的场景，使用Spark SQL从hive中查询数据时
        // Spark SQL会根据hive对应的hdfs文件的block数量还决定加载出来的数据rdd有多少个partition
        // 这里的partition数量，是我们根本无法设置的

        // 有些时候，可能它自动设置的partition数量过于少了，导致我们后面的算子的运行特别慢
        // 此时就可以在Spark SQL加载hive数据到rdd中以后
        // 立即使用repartition算子，将rdd的partition数量变多

        // 案例
        // 公司要增加新部门
        // 但是人员还是这么多，所以我们只能使用repartition操作，增加部门
        // 将人员平均分配到更多的部门中去
        JavaRDD<String> staffWithDepartment_6 = staffWithDepartment_2.repartition(6);
        System.out.println("============================coalesce后======================================");

        JavaRDD<String> staffWithDepartment_6_2 = staffWithDepartment_6.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Iterator<String> call(Integer department, Iterator<String> it) throws Exception {
                List<String> staffs_ddpartment_dapartments = new ArrayList<String>();
                while(it.hasNext()){
                    String staff_department = it.next();
                    staffs_ddpartment_dapartments.add("【部门"+(department+1)+"】："+staff_department);
                }
                return staffs_ddpartment_dapartments.iterator();
            }
        }, true);

        for(String staff:staffWithDepartment_6_2.collect()){
            System.out.println(staff);
        }

        sc.close();
    }
}
