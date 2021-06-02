package spark.study.java.sql;

import org.apache.log4j.spi.LoggerFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;
import java.util.logging.Logger;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/16.
 * 每日Top3热点搜索词统计案例
 */
public class DailyTop3Keyword {
    public static void main(String[] args) {
        //创建SparkConf,JavaSparkContext,HiveContext对象
        SparkConf conf = new SparkConf()
                .setAppName("每日Top3热点搜索词统计案例")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //这个程序是要放到集群上跑的，所以spark的lib中放一个mysql-connection.jar和conf中放一个hive.site.xml配置文件
        HiveContext hiveContext = new HiveContext(sc.sc());

        //创建字典并广播出去，每个节点只拷贝一份
        Map<String,List<String>> paramMap = new HashMap<String,List<String>>();
        paramMap.put("city", Arrays.asList("beijing"));
        paramMap.put("platform",Arrays.asList("android"));
        paramMap.put("version",Arrays.asList("1.0","1.2","1.5","2.0"));
        final Broadcast<Map<String, List<String>>> queryParamMapBroadcast = sc.broadcast(paramMap);

        //读取hdfs上的文件，创建初始化RDD
        //JavaRDD<String> lineRDD = sc.textFile("hdfs://spark1:9000/usr/local/spark-study/resources/keyword.txt");
        JavaRDD<String> lineRDD = sc.textFile("D:\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）\\Spark从入门到精通（Scala编程、案例实战、高级特性、Spark内核源码剖析、Hadoop高端）\\第87讲-Spark SQL：与Spark Core整合之每日top3热点搜索词统计案例实战\\第87讲-Spark SQL：与Spark Core整合之每日top3热点搜索词统计案例实战\\文档\\keyword.txt");
        //过滤出满足包含city,platform,version的字段的数据，不满足的过滤掉
        JavaRDD<String> filteredRDD = lineRDD.filter(new Function<String, Boolean>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Boolean call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];
                Map<String, List<String>> queryParamMap = queryParamMapBroadcast.value();
                List<String> cities = queryParamMap.get("city");
                if(!cities.contains(city)){
                    return false;
                }
                List<String> platfrms = queryParamMap.get("platform");
                if(!platfrms.contains(platform)){
                    return false;
                }
                List<String> versions = queryParamMap.get("version");
                if (!versions.contains(version)){
                    return false;
                }
                return true;
            }
        });
        //把lineRDD----->PairRDD
        JavaPairRDD<String, String> dailyKeywrdUser = filteredRDD.mapToPair(new PairFunction<String, String, String>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(String log) throws Exception {
                String[] logSplited = log.split("\t");
                String date = logSplited[0];
                String user = logSplited[1];
                String keyword = logSplited[2];
                return new Tuple2<String, String>(date + "_" + keyword, user);
            }
        });

        //根据dateKeyword分组(user未去重)
        JavaPairRDD<String, Iterable<String>> userGroupedBydailyKeyword = dailyKeywrdUser.groupByKey();
        
        //针对每天每个搜索词的用户执行去重操作,获取uv
        JavaPairRDD<String, Long> dateKeywordUv = userGroupedBydailyKeyword.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String dateKeyword = t._1;
                //对用户执行去重操作
                List<String> distinctUsers = new ArrayList<String>();
                Iterator<String> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    String user = iterator.next();
                    if (!distinctUsers.contains(user)) {
                        distinctUsers.add(user);
                    }
                }
                long uv = distinctUsers.size();

                return new Tuple2<String, Long>(dateKeyword, uv);
            }
        });

        //将每天每个搜索词的uv转化为DataFrame
        JavaRDD<Row> dateKeywordUvRow = dateKeywordUv.map(new Function<Tuple2<String, Long>, Row>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Row call(Tuple2<String, Long> t) throws Exception {
                String date = t._1.split("_")[0];
                String keyWord = t._1.split("_")[1];
                long uv = t._2;
                return RowFactory.create(date, keyWord, uv);
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("date",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("keyword",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("uv",DataTypes.LongType,true));
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame dataKeywordUvDF = hiveContext.createDataFrame(dateKeywordUvRow, structType);

        //注册临时表，查询每天，每个搜索词的top3
        dataKeywordUvDF.registerTempTable("date_keyword_uv");
        DataFrame dailyTop3UvDF = hiveContext.sql(""
                + "select date,keyword,uv "
                + "from ( "
                + "select "
                + "date, "
                + "keyword,"
                + "uv, "
                + "row_number() over (partition by date order by uv desc) rank "
                + "from date_keyword_uv"
                + " ) temp "
                + "where rank <=3"
        );

        //测试一下数据------结果没问题
        dailyTop3UvDF.show();

        //将DataFrame转化为RDD，然后映射，计算出每天Top3的搜索词的uv总数
        JavaRDD<Row> dateKeyWordUvRow = dailyTop3UvDF.javaRDD();
        JavaPairRDD<String, String> top3DateKeywordUv = dateKeyWordUvRow.mapToPair(new PairFunction<Row, String, String>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                //以下这种方式获取的数据有问题,顺序会打乱
                //同时row.get()这种方式得到的是一个object类型的数据，后面必须要进行转化
                String date = row.getString(0);
                String keyword = row.getString(1);
                long uv = row.getLong(2);
//                String date = String.valueOf(row.get(0));
//                String keyword = String.valueOf(row.get(1));
//                long uv = Long.valueOf(String.valueOf(row.get(2)));
                return new Tuple2<String, String>(date, keyword + "_" + uv);
            }
        });
        //测试数据
        top3DateKeywordUv.foreach(new VoidFunction<Tuple2<String, String>>() {
            private static final long seriaversionUID = 1L;
            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        JavaPairRDD<String, Iterable<String>> top3KeywordUvByDate = top3DateKeywordUv.groupByKey();

        JavaPairRDD<Long, String> dailyTop3TotalUv = top3KeywordUvByDate.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                String date = t._1;
                Long totalUv = 0L;
                String date_keywordUv = date;
                Iterator<String> iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    String keyWordUv = iterator.next();
                    totalUv += Long.valueOf(keyWordUv.split("_")[1]);
                    date_keywordUv += "," + keyWordUv;
                }
                return new Tuple2<Long, String>(totalUv, date_keywordUv);
            }
        });
        //按照每天的总搜索uv降序排序
        JavaPairRDD<Long, String> sortedByDailyTop3TotalUv = dailyTop3TotalUv.sortByKey(false);

        //再次进行映射，将排序后的数据，映射回原始的格式 Iterable<Row>
        JavaRDD<Row> dailyTop3KeyWordUv = sortedByDailyTop3TotalUv.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            private static final long seriaVersionUID = 1L;

            @Override
            public Iterable<Row> call(Tuple2<Long, String> t) throws Exception {
                String date_keyWord_uv = t._2();
                String[] splitedDateKeyWordUv = date_keyWord_uv.split(",");
                String date = splitedDateKeyWordUv[0];
                List<Row> rows = new ArrayList<Row>();
                rows.add(RowFactory.create(date, splitedDateKeyWordUv[1].split("_")[0], Long.valueOf(splitedDateKeyWordUv[1].split("_")[1])));
                rows.add(RowFactory.create(date, splitedDateKeyWordUv[2].split("_")[0], Long.valueOf(splitedDateKeyWordUv[2].split("_")[1])));
                rows.add(RowFactory.create(date, splitedDateKeyWordUv[3].split("_")[0], Long.valueOf(splitedDateKeyWordUv[3].split("_")[1])));

                return rows;
            }
        });

        DataFrame dailyTop3KeyWordUvDF = hiveContext.createDataFrame(dailyTop3KeyWordUv, structType);
        //dailyTop3KeyWordUvDF.show();

        //这句代码是上传到集群上运行时，会把结果存储到hive表中
        //dailyTop3KeyWordUvDF.saveAsTable("daily_top3_keyword_uv");

        //释放资源
        sc.close();


    }
}
