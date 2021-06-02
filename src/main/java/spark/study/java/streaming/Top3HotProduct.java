package spark.study.java.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * author:liangsir
 * qq:714628767
 * created 2019/01/22.
 * 与SparkSQL整合，实现top3热门商品实时统计
 */
public class Top3HotProduct {
    public static void main(String[] args) {
        //创建实例
        SparkConf conf = new SparkConf()
                .setAppName("Top3HotProduct")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
        //这里的日志格式是 user product category
        //创建实时数据输入流
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("spark1",9999);
        //映射为pairDSream的格式
        JavaPairDStream<String,Integer> categoryProductAndOne = lines.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                String [] lineSplited = line.split(" ");
                String product = lineSplited[1];
                String category = lineSplited[2];
                return new Tuple2<String, Integer>(category+"_"+product,1);
            }
        });
        //因为是统计最近60秒内的商品top，所以聚合的时候要用滑动窗口函数------>注意每60秒的RDD合并为一个大RDD
        //三个参数，第一个参数是实现聚合的匿名函数，第二个参数是滑动窗口的大小60秒，第二个参数是滑动窗口的间隔时间
        JavaPairDStream<String,Integer> categoryProductCounts = categoryProductAndOne.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));


        categoryProductCounts.print();
        //采用sparkSQL的方式来实现分组Toop3的计算统计转化
        categoryProductCounts.foreachRDD(new Function<JavaPairRDD<String, Integer>, Void>() {
            private static final long seriaVersionUID = 1L;
            @Override
            public Void call(JavaPairRDD<String, Integer> categoryProductCountsRDD) throws Exception {
                //其实这里的每一个RDD就是60秒的大RDD----把这个RDD 拆封转为为RowRDD
                JavaRDD<Row> categoryProductCountsRowRDD = categoryProductCountsRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                    private static final long seriaVerionUID = 1L;
                    @Override
                    public Row call(Tuple2<String, Integer> tp) throws Exception {
                        String [] categoryProductSplited = tp._1.split("_");
                        String category = categoryProductSplited[0];
                        String product = categoryProductSplited[1];
                        Integer count = tp._2;
                        return RowFactory.create(category,product,count);
                    }
                });
                //构建Schema信息----也就是构建StructTpe
                List<StructField> structFields = new ArrayList<StructField>();
                structFields.add(DataTypes.createStructField("category",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("product",DataTypes.StringType,true));
                structFields.add(DataTypes.createStructField("click_count",DataTypes.IntegerType,true));
                StructType structType = DataTypes.createStructType(structFields);

                //方式一
                //通过Row+Schema创建DataFrame
                //注意categoryProductCountsRDD是源头，所以要通过获取上下文实例
                //必须放到集群中去跑,好像这个版本只能用hiveContext，不能用sqlContext
                HiveContext hiveContext = new HiveContext(categoryProductCountsRDD.context());
                DataFrame cateProCountDF = hiveContext.createDataFrame(categoryProductCountsRowRDD, structType);

                //注册临时表
                cateProCountDF.registerTempTable("cateProCountTab");
                //写查询的sql语句
                String sql = ""
                        +"select category,product,click_count "
                        +"from ( "
                            +"select "
                                +"category ,"
                                +"product ,"
                                +"click_count , "
                                +"row_number() over (partition by category order by click_count desc) rank "
                            +"from "
                                +"cateProCountTab "
                        +") temp "
                        +"where rank <= 3";

                //DataFrame top3GroupByCategory = hiveContext.sql(sql);
                DataFrame top3GroupByCategory = hiveContext.sql(sql);
                top3GroupByCategory.show();

                return null;
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
