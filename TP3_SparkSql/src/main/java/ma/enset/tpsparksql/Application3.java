package ma.enset.tpsparksql;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.col;

public class Application3 {
    private static Employe Employe;

    public static void main(String[] args) {
        SparkSession ss=SparkSession.builder().
                appName("TP Spark SQL").
                master("local[*]").getOrCreate();

        SparkSession sparkSession = SparkSession.builder().appName("TP Spark SQL-> DataSet<- ").master("local[*]").getOrCreate();

        Encoder<Employe> employeeBeanEncoder = Encoders.bean(Employe.class);
        Dataset<Employe> ds = ss.read().option("multiline", true).json("employes.json").as(employeeBeanEncoder);
        ds.printSchema();

        System.out.println(" ==> age between 30 & 35 : ");
        ds.filter((FilterFunction<Employe>) employeeBean -> employeeBean.getAge()>=30 && employeeBean.getAge()<=35).show();


    }
    }
