package org.masterinformatica.sparkstructuredstreaming;

import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class StreamingWordCount {

  public static void main(String[] args) throws InterruptedException, StreamingQueryException {
   /* if (args.length < 2) {
      System.err.println("Usage: StreamingWordCount <hostname> <port>");
      System.exit(1);
    }*/

    Logger.getLogger("org").setLevel(Level.OFF) ;

    // Create the context with a 1 second batch size
    SparkSession sparkSession = SparkSession
        .builder().master("local")
        .getOrCreate() ;

    Dataset<Row> lines = sparkSession
        .readStream()
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load();

    lines.printSchema();

    Dataset<String> words = lines
        .as(Encoders.STRING())
        .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator(), Encoders.STRING()) ;

    Dataset<Row> wordCounts = words
        .groupBy("value")
        .count() ;

    wordCounts.printSchema();

    StreamingQuery query = wordCounts
        .writeStream()
        .outputMode("update")
       // .outputMode("complete")
        .format("console")
        .start();

    query.awaitTermination();
  }
}
