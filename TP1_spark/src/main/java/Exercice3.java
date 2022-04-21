import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Exercice3 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("word count").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("1750.csv");
        JavaRDD<String> rdd2=rdd1.flatMap( s-> Arrays.asList(s.split("\n") ).iterator());

        JavaRDD<Double> valeursTmp_max = rdd2.filter( s -> s.split(",")[2].equals("TMAX")  ).map(s -> Double.parseDouble(s.split(",")[3]));
        long countmax = valeursTmp_max.count();
        double summax = valeursTmp_max.reduce(Double::sum);
        System.out.println("moyenne des temperatures max="+ (summax/countmax) );

        JavaRDD<Double> valeursTmp_min = rdd2.filter( s -> s.split(",")[2].equals("TMIN")).map(s -> Double.parseDouble(s.split(",")[3]));
        long countmin = valeursTmp_max.count();
        double summin = valeursTmp_max.reduce(Double::sum);
        System.out.println("moyenne des temperatures min="+ (summin/countmin) );

        JavaRDD<Double> max = rdd2.filter( s -> s.split(",")[2].equals("TMAX")  ).map(s -> Double.parseDouble(s.split(",")[3]));
        double Tmax= valeursTmp_max.reduce(Double::max);
        System.out.println("Température maximale la plus élevée="+ Tmax);

        JavaRDD<Double> min = rdd2.filter( s -> s.split(",")[2].equals("TMIN")  ).map(s -> Double.parseDouble(s.split(",")[3]));
        double Tmin= valeursTmp_min.reduce(Double::min);
        System.out.println("Température minimale la plus basse="+ Tmin);


        JavaPairRDD<String, Double> rdd4 = rdd2.filter(s -> s.split(",")[2].equals("TMAX")).mapToPair(s -> new Tuple2<>(s.split(",")[0],Double.parseDouble(s.split(",")[3])));
        JavaPairRDD<String, Double>  rdd5=rdd4.reduceByKey((v1,v2)-> v1>v2? v1:v2);
        JavaPairRDD<Double,String> rdd6=rdd5.mapToPair(s->{
            return new Tuple2<>(s._2,s._1);
        });
        JavaPairRDD<Double,String> rdd7=rdd6.sortByKey(false);
        List<Tuple2<Double,String>> top5Max=rdd7.take(5);
        System.out.printf("les 5 top station les plus chauds sont :\n");
        top5Max.forEach(a-> System.out.println(a._2()+" "+a._1()));

        JavaPairRDD<String, Double> rdd8 = rdd2.filter(s -> s.split(",")[2].equals("TMIN")).mapToPair(s -> new Tuple2<>(s.split(",")[0],Double.parseDouble(s.split(",")[3])));
        JavaPairRDD<String, Double>  rdd9=rdd8.reduceByKey(Double::min);
        JavaPairRDD<Double,String> rdd10=rdd9.mapToPair(s->{
            return new Tuple2<>(s._2,s._1);
        });
        //(v1,v2)-> v1<v2? v1:v2

        JavaPairRDD<Double,String> rdd11=rdd10.sortByKey(true);

        List<Tuple2<Double,String>> top5Min=rdd11.take(5);
        System.out.printf("les 5 top station les plus froids sont :\n");
        top5Min.forEach(a-> System.out.println(a._2()+" "+a._1()));
    }
}
