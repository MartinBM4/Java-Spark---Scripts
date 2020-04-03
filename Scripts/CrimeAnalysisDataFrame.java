package org.masterinformatica.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class CrimeAnalysisDataFrame {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.OFF);

    SparkSession sparkSession =
        SparkSession.builder()
            .appName("Spark program to process a CSV file")
            .master("local[8]")
            .getOrCreate();

    long startTime = System.currentTimeMillis();

    Dataset<Row> dataFrame =
        sparkSession
            .read()
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("data/Police_Department_Incident_Reports__2018_to_Present.csv")
            .cache();

    dataFrame.printSchema();
    dataFrame.show();

    Dataset<Row> categories =
        dataFrame
                .groupBy("Police District")
                .count()
                .sort(col("count").desc());

    categories.show(10);

    long totalComputingTime = System.currentTimeMillis() - startTime;

    System.out.println("Computing time: " + totalComputingTime);
  }
}
