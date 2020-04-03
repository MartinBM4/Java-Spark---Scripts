package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
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


public class Filmlocation {

    public static void main(String[] args)  {
        Logger.getLogger("org").setLevel(Level.OFF);


        SparkConf sparkConf = new SparkConf().setAppName("Spanish Airports").setMaster("local[4]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/Film_Locations_in_San_Francisco.csv");

        JavaRDD<String[]> linesSplit = lines
                .map(line -> line.split(","));

        JavaRDD<String> lineconv = linesSplit.map(line -> line[0]);

        JavaPairRDD<String, Integer> linemapped = lineconv.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairRDD<String, Integer> listcounted = linemapped.reduceByKey((a,b) -> a+b);

        JavaPairRDD<String, Integer> output1 = listcounted.filter(w -> w._2 >= 20);

        List<Tuple2<String, Integer>> listsorted = output1
                .sortByKey(false)
                .take(5);

        for (Tuple2<?, ?> tuple : listsorted) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }



        JavaPairRDD<String, Integer> output2 = linemapped.reduceByKey((a, b) -> a);
        double x = output2.count();

        System.out.println(x);

        JavaDoubleRDD doble =listcounted.mapToDouble(n -> n._2);
        Double media = doble.mean();

        System.out.println(media);

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