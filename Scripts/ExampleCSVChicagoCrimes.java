package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class ExampleCSVChicagoCrimes {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);

        SparkSession sparkSession =
                SparkSession.builder()
                        .appName("Spark program to process a CSV file")
                        .master("local[8]")
                        .getOrCreate();
        
        Dataset<Row> dataFrame =
                sparkSession
                        .read()
                        .format("csv")
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .load("home/master/Descargas/Film_Locations_in_San_Francisco.csv")
                        .cache();

        dataFrame.printSchema();
        dataFrame.show();
    }
}
