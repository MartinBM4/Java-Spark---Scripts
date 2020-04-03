package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.time.Instant;
import java.util.Locale;

import static java.lang.Integer.valueOf;

/**
 * Program that sums mumbers contained in files using Apache Spark
 * @author Antonio J. Nebro
 */
public class AddNumbersFromFiles {
  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.OFF) ;

    // Step 1: create a SparkConf object
    SparkConf conf = new SparkConf()
            .setAppName("Add numbers from files")
            .setMaster("local[4]") ; ;

    // Step 2: create a Java Spark Context
    JavaSparkContext context = new JavaSparkContext(conf) ;

    long startComputingTime = Instant.now().toEpochMilli() ;

    // Step 3: Perform the computation
    int sum = context
            .textFile(args[0])
            .map(line -> valueOf(line))
            .reduce((integer, integer2) -> integer + integer2) ;

    /* The Step 3 is a compact alternative to the following sentences:*/
    /*
    JavaRDD<String> lines = context.textFile(args[0]) ;
    JavaRDD<Integer> numbers = lines.map(number -> valueOf(number)) ;
    int sum = numbers.reduce((integer, integer2) -> integer + integer2) ;
    */

    System.out.println("Sum: " + sum);

    // Step 4: stop the spark context
    context.stop() ;

    System.out.println("Computing time (ms): " + (Instant.now().toEpochMilli() - startComputingTime)) ;
  }
}
