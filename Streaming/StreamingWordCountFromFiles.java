package org.masterinformatica.sparkstructuredstreaming;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

public class StreamingWordCountFromFiles {

  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
    if (args.length < 1) {
      System.err.println("Usage: StreamingWordCountFromFiles <streaming directory>");
      System.exit(1);
    }

    Logger.getLogger("org").setLevel(Level.OFF);

    // Create the context with a 1 second batch size
    SparkSession sparkSession = SparkSession
            .builder()
            .appName("SparkStructuredStreaming")
            .master("local[4]")
            .getOrCreate();

    Dataset<Row> dataFrame = sparkSession
            .readStream()
            .format("text")
            .load(args[0]);

    Dataset<String> words = dataFrame
            .as(Encoders.STRING())
            .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING());

    Dataset<Row> wordCounts = words
            .groupBy("value")
            .count();

    wordCounts.printSchema();

    StreamingQuery query = wordCounts
            .writeStream()
            //.outputMode("update")
            .outputMode("complete")
            .format("console")
            .start();

    query.awaitTermination();
  }
}
