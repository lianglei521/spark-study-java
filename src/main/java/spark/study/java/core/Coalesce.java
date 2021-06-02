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
public class Coalesce {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Coalesce")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<String> staffList = Arrays.asList("张三","李四","王五","赵六","琼华","公主雅","皓澜","秦王妃","若曦","兰妃");
        JavaRDD<String> staffRDD = sc.parallelize(staffList,6);

        //一定要记住mapPartitionsWithIndex这个算子有两个参数
        JavaRDD<String> staffWithDepartment_6 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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

        for(String staff: staffWithDepartment_6.collect()){
            System.out.println(staff);
        }

        // coalesce算子，功能是将RDD的partition缩减，减少
        // 将一定量的数据，压缩到更少的partition中去

        // 建议的使用场景，配合filter算子使用
        // 使用filter算子过滤掉很多数据以后，比如30%的数据，出现了很多partition中的数据不均匀的情况
        // 此时建议使用coalesce算子，压缩rdd的partition数量
        // 从而让各个partition中的数据都更加的紧凑

        // 公司原先有6个部门
        // 但是呢，不巧，碰到了公司裁员，裁员以后呢，有的部门中的人员就没了
        // 不同的部分人员不均匀
        // 此时呢，做一个部门整合的操作，将不同的部门的员工进行压缩
        JavaRDD<String> staffWithDepartment_3 = staffWithDepartment_6.coalesce(3);
        System.out.println("============================coalesce后======================================");

        JavaRDD<String> staffWithDepartment_3_6 = staffWithDepartment_3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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

        for(String staff:staffWithDepartment_3_6.collect()){
            System.out.println(staff);
        }

        sc.close();
    }
}
