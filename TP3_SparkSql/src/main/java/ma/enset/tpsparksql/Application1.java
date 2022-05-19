package ma.enset.tpsparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class Application1 {
    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();

        Dataset<Row> df=ss.read().option("multiLine",true).json("students.json");

        df.show();

        df.printSchema();

        df.select("name","note").show();

        // perform actions on cols
        df.select( col("name"), col("note").minus(20) ).show();

        df.filter(col("note").gt(15).and(col("name").lt(20))).show();

        df.createOrReplaceTempView("students");
        ss.sql("select name from students where note >= 10 ").show();

    }
}
