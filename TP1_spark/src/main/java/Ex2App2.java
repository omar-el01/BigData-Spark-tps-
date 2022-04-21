import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class Ex2App2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> rdd1=sc.textFile("Ventes.txt");

        JavaRDD<String> rdd2=rdd1.flatMap(s-> Arrays.asList(s.split("\n")).iterator());

        JavaPairRDD<String,Double> rdd3=rdd2.mapToPair(s->new Tuple2<>(s.split(" ")[0]+" "+s.split(" ")[2],Double.parseDouble(s.split(" ")[3])));

        JavaPairRDD<String,Double> rdd4=rdd3.reduceByKey((v1, v2)->v1+v2);
        rdd4.foreach(nameTuple-> System.out.println(nameTuple._1()+" "+nameTuple._2()));
    }
    }