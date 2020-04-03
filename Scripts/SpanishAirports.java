package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;


public class SpanishAirports {

    public static void main(String[] args)  {
        Logger.getLogger("org").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spanish Airports").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/airports.csv");

        JavaRDD<String[]> linesSplit = lines
                .map(line -> line.split(","));

        JavaRDD<String[]> lineFiltered = linesSplit.filter(line -> line[8].contains("ES"));

        JavaRDD<String> lineconv = lineFiltered.map(line -> line[2]);

        JavaPairRDD<String, Integer> linemapped = lineconv.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> listcounted = linemapped.reduceByKey((a,b) -> a+b);

        List<Tuple2<String, Integer>> listsorted = listcounted
                .sortByKey(false)
                .collect();

        for (Tuple2<?, ?> tuple : listsorted) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        /*
        File fileToSave = new File("data/type_airports.txt");
        FileWriter writing =new FileWriter(fileToSave,true);
        writing.write(listcounted.toString());
        writing.close();
        */
        // Stop the spark context
        sparkContext.stop();
    }
}