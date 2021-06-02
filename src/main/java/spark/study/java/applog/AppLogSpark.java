package spark.study.java.applog;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/02/05.
 * 移动端app访问流量日志案例分析
 */
public class AppLogSpark {
    public static void main(String[] args) {
        //创建spark配置和上下文对象
        SparkConf conf = new SparkConf()
                .setAppName("移动端app访问流量日志案例分析")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 读取日志文件，并创建一个RDD
        // 使用SparkContext的textFile()方法，即可读取本地磁盘文件，或者是HDFS上的文件
        // 创建出来一个初始的RDD，其中包含了日志文件中的所有数据
        JavaRDD<String> accessLogRDD = sc.textFile("./src/main/java/spark/study/java/applog/access.log");

        //将日志格式映射为key-value格式的RDD,为后面的reduceByKey做聚合准备
        JavaPairRDD<String,AccessLogInfo> accessLogPairRDD = mapAccessLogRDD2Pair(accessLogRDD);

        //根据deviceId进行聚合计算出每个device的总上行流量，总下行流量，以及最早的访问时间
        JavaPairRDD<String,AccessLogInfo> accessLogInfoByDeviceId = aggregateByDeviceId(accessLogPairRDD);

        //将key映射为二次排序key的RDD
        JavaPairRDD<AccessLogSortKey,String> accessLogSortKeyStringJavaPairRDD = mapKey2SortKey(accessLogInfoByDeviceId);

        //执行二次排序操作，按照上行流量，下行流量，时间戳进行倒序排序
        JavaPairRDD<AccessLogSortKey,String> sortedAccessLogPairRDD = accessLogSortKeyStringJavaPairRDD.sortByKey(false);

        //取Top10
        List<Tuple2<AccessLogSortKey,String>> top10AccessLog = sortedAccessLogPairRDD.take(10);

        //打印结果
        for(Tuple2<AccessLogSortKey,String> tp : top10AccessLog){
            System.out.println(tp._2 + ":" + tp._1);
        }

        //关闭spark上下文
        sc.close();
    }

    /**
     * 将日志RDD映射为key-value的格式
     * @param accessLogRDD 日志RDD
     * @return key-value格式RDD
     */
    private static JavaPairRDD<String,AccessLogInfo> mapAccessLogRDD2Pair(JavaRDD<String> accessLogRDD){
        return accessLogRDD.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, AccessLogInfo> call(String accessLogRDD) throws Exception {
                // 根据\t对日志进行切分
                String[] splited = accessLogRDD.split("\\t");
                //获取四个字段
                long timestamp = Long.parseLong(splited[0]);
                String deviceId = splited[1];
                long upTraffic = Long.parseLong(splited[2]);
                long downTraffic = Long.parseLong(splited[3]);
                //将时间戳、上行流量、下行流量，封装为自定义的可序列化对象
                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
                return new Tuple2<String, AccessLogInfo>(deviceId,accessLogInfo);
            }
        });
    }

    /**
     * 根据diviceId进行聚合操作
     * 计算出每个device的总上行流量，总下行流量，以及最早的访问时间
     * @param accessLogPairRDD，日志key-value格式的RDD
     * @return 按device聚合RDD
     */
    private static JavaPairRDD<String,AccessLogInfo> aggregateByDeviceId(JavaPairRDD<String,AccessLogInfo> accessLogPairRDD){
       return accessLogPairRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public AccessLogInfo call(AccessLogInfo accessLogInfo1, AccessLogInfo accessLogInfo2) throws Exception {
                long timestamp = accessLogInfo1.getTimestamp() < accessLogInfo2.getTimestamp()? accessLogInfo1.getTimestamp():accessLogInfo2.getTimestamp();
                long upTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getUpTraffic();
                long downTraffic = accessLogInfo1.getDownTraffic() + accessLogInfo2.getDownTraffic();
                return new AccessLogInfo(timestamp,upTraffic,downTraffic);
            }
        });
    }

    /**
     * 将RDD的key映射为二次排序key
     * @param accessLogInfoJavaPairRDD
     * @return 二次排序key的 RDD
     */
    private static JavaPairRDD<AccessLogSortKey,String> mapKey2SortKey(JavaPairRDD<String,AccessLogInfo> accessLogInfoJavaPairRDD){
        return accessLogInfoJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, AccessLogInfo>, AccessLogSortKey, String>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> tp) throws Exception {
                String deviceId = tp._1;
                AccessLogInfo accessLogInfo = tp._2;
                AccessLogSortKey accessLogSortKey = new AccessLogSortKey();
                accessLogSortKey.setUpTraffic(accessLogInfo.getUpTraffic());
                accessLogSortKey.setDownTraffic(accessLogInfo.getDownTraffic());
                accessLogSortKey.setTimestamp(accessLogInfo.getTimestamp());
                return new Tuple2<AccessLogSortKey, String>(accessLogSortKey,deviceId);
            }
        });
    }
}
