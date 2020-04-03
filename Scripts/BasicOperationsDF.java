package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import javax.xml.crypto.Data;

import java.util.List;

import static org.apache.spark.sql.functions.*;

public class BasicOperationsDF {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.OFF);

    SparkSession sparkSession =
        SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .master("local[8]")
            .getOrCreate();

    Dataset<Row> dataFrame =
        sparkSession.read().format("csv").option("header", "true").load("/home/master/Descargas/ejemploCSV.csv").cache();

    dataFrame.printSchema();
    dataFrame.show();

    System.out.println("data types: " + dataFrame.dtypes());

    // Describe the dataframe
    dataFrame.describe().show();

    // Explain the dataframe
    dataFrame.explain();

    // Select everything
    dataFrame.select("Name").show();

    // Select columns name and age, but adding 1 to age
    dataFrame.select(col("Name"), col("Age").plus(1)).show();

    // Select the rows having a name length > 4
    dataFrame.select(functions.length(col("Name")).gt(4)).show();

    // Select names staring with L
    dataFrame.select(col("name"), col("name").startsWith("L")).show();

    // Add a new column "Senior" containing true if the person age is > 45
    dataFrame.withColumn("Senior", col("Age").gt(45)).show();

    // Rename column HasACar as Owner
    dataFrame.withColumnRenamed("HasACar", "Owner").show();

    // Remove column DateBirth
    dataFrame.drop("BirthDate").show();

    // Sort by age
    dataFrame.sort(col("Age").desc()).show();

    dataFrame.orderBy(col("Age").desc(), col("Weight")).show();

    // Get a RDD
    JavaRDD<Row> rddFromDataFrame = dataFrame.javaRDD().persist(StorageLevel.MEMORY_AND_DISK());

    for (Row row : rddFromDataFrame.collect()) {
      System.out.println(row);
    }

    // Sum all the weights (RDD)
    double sumOfWeights =
        rddFromDataFrame.map(row -> Double.valueOf((String) row.get(2))).reduce((x, y) -> x + y);

    System.out.println("Sum of weights (RDDs): " + sumOfWeights);

    // Sum all the weights (dataframe)
    dataFrame.select("Weight").groupBy().sum().show();
    dataFrame.select(functions.sum("weight")).show();
    dataFrame.agg(sum("weight")).show();

    System.out.println(
        "Sum of weights (Dataframe): "
            + dataFrame.agg(sum("weight")).collectAsList().get(0).get(0));

    // Get the mean age (RDD)
    int totalAge =
        rddFromDataFrame.map(row -> Integer.valueOf((String) row.get(1))).reduce((x, y) -> x + y);

    double meanAge = 1.0 * totalAge / rddFromDataFrame.count();

    System.out.println("Mean age (RDDs): " + meanAge);

    // Get the mean age (dataframe)
    dataFrame.select(functions.avg("Weight")).withColumnRenamed("avg(Weight)", "Average").show();

    // Write to a json file
    dataFrame.write().json("output.json");

    // Write to a CSV file
    dataFrame.write().csv("output.csv");
  }
}
