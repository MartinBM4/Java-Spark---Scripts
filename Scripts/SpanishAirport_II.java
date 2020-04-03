package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple1;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.io.File;
import java.io.FileWriter;

public class SpanishAirport_II {
    static Logger log = Logger.getLogger(SpanishAirports.class.getName());

    public static void main(String[] args) throws IOException {
        Logger.getLogger("org").setLevel(Level.OFF);

        // Create a SparkConf object
        if (args.length < 1) {
            log.fatal("Syntax Error: there must be one argument (a file name or a directory)");
            throw new RuntimeException();
        }

        // Create a SparkConf object
        SparkConf sparkConf = new SparkConf()
                .setAppName("SpanishAirports")
                .setMaster("local[4]");

        // Create a Java Spark context and Read lines
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sparkContext.textFile(args[0]);

        /*
         Split the lines ("map") Filter by iso_country code = ES (Spain)("filter") Map pairs by type of airports("mapToPair")
        Reduce operation that sum the values of all the pairs having the same key ("type of airport"),generating
        a pair <key, sum>("reduceByKey")
        */
        JavaPairRDD<String, Integer> output = lines
                .map(line -> Arrays.asList(line.split(",")))
                .filter(line -> line.get(8).equals("\"ES\""))
                .mapToPair(line -> new Tuple2<>(line.get(2), 1))
                .reduceByKey((integer, integer2) -> integer + integer2);


        // Sort the results by key and take the collection
        List<Tuple2<String, Integer>> info = output
                .sortByKey(false)
                .collect();

        // Print the results
        for (Tuple2<?, ?> tuple : info) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        File fileToSave = new File("data/type_airports.txt");
        FileWriter writing =new FileWriter(fileToSave,true);
        writing.write(output.toString());
        writing.close();

        // Stop the spark context
        sparkContext.stop();
    }
}

