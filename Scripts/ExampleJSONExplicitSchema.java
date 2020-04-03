package org.masterinformatica.sparksql;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class ExampleJSONExplicitSchema {

  public static void main(String[] args) {

    Logger.getLogger("org").setLevel(Level.OFF);

    SparkSession sparkSession =
        SparkSession.builder()
            .appName("Java Spark SQL basic example")
            .master("local[2]")
            .getOrCreate();

    StructType explicitSchema =
        new StructType()
            .add("borough", "string")
            .add("cuisine", "string")
            .add("name", "string")
            .add("restaurant_id", "string");

    Dataset<Row> dataFrame =
        sparkSession
            .read()
            .schema(explicitSchema)
            .json("data/primer-dataset.json") ;

    dataFrame.printSchema();
    dataFrame.show(100);
  }
}
