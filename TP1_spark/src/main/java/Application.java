import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<Integer> rdd1=sc.parallelize(Arrays.asList(12,10,9,20,39,8));
        JavaRDD<Integer> rdd2=rdd1.map(a -> a+1);
        //rdd2.foreach(a-> System.out.println("mapping "+ a));
        JavaRDD<Integer> rdd3=rdd2.filter(a -> a>10);
        //rdd3.foreach(a-> System.out.println("a>10 "+ a));
        JavaRDD<Integer> rdd4=rdd2.filter(a -> a<20);
        //rdd4.foreach(a-> System.out.println("a<20 "+ a));
        JavaRDD<Integer> rdd5=rdd2.filter(a -> a>9);
        //rdd5.foreach(a-> System.out.println("a>9 "+ a));
        JavaRDD<Integer> rdd6=rdd3.union(rdd4);
        //rdd6.foreach(a-> System.out.println("union "+ a));
        JavaPairRDD<Integer,Integer> rdd71=rdd5.mapToPair(a -> new Tuple2<>(a,1));
        //rdd71.foreach(a-> System.out.println("maping by key"+ a));
        JavaPairRDD<Integer, Integer> rdd81=rdd6.mapToPair(a ->new Tuple2<>(a,-1));
        //rdd81.foreach(a-> System.out.println("maping by key  "+ a));
        JavaPairRDD<Integer, Integer> rdd8=rdd81.reduceByKey((a, b) ->a+b );
        //rdd8.foreach(a-> System.out.println("reducing rdd81 "+ a));
        JavaPairRDD<Integer, Integer> rdd7=rdd71.reduceByKey((a, b) ->a+b );
        //rdd7.foreach(a-> System.out.println("reducing rdd71 "+ a));
        JavaPairRDD<Integer, Integer> rdd9=rdd7.union(rdd8);
        //rdd9.foreach(a-> System.out.println("union rdd7 et rdd8 "+ a));
        JavaPairRDD<Integer, Integer> rdd10=rdd9.sortByKey(true);

        rdd10.foreach(a-> System.out.println(a));

    }
}
