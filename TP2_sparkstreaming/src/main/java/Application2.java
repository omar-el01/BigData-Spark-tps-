import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Application2 {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("Spark Streaming HDfs").setMaster("local[*]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(30));
        JavaDStream<String> lines=jsc.textFileStream("hdfs://localhost:9000/TPspark/");

        lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaDStream<String> words=lines.flatMap(s-> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String,Integer> wordspair=words.mapToPair(s -> new Tuple2<String, Integer>(s,1));
        JavaPairDStream<String,Integer>  wordcount=wordspair.reduceByKey((n1,n2)->n1+n2);
        wordcount.print();
        jsc.start();
        jsc.awaitTermination();
    }
}
