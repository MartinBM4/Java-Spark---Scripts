//Álvaro Florín Vega
package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

public class SumColumn_DF {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession= SparkSession.builder().appName("Java Spark SQL basic example")
                .master("local[4]").getOrCreate();
        Dataset<String> dataFrame =
                sparkSession
                        .read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("delimiter","\n")
                        .textFile("/home/master/modulo8/numbers.txt")
                        .cache();
        dataFrame.show();

        double suma= (double) dataFrame.select(functions.sum("value")).withColumnRenamed("sum(value)","suma")
                .collectAsList().get(0).get(0);

        System.out.println(suma);
        sparkSession.close();
    }
}
