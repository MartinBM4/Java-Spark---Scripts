package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers using Apache Spark
 *
 * @author Antonio J. Nebro
 */
public class AddNumbers {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        // Step 1: create a SparkConf object
        SparkConf sparkConf = new SparkConf().setAppName("Add numbers").setMaster("local[4]");
        //configurar entorno de la aplicación, le indico que voy a trabajar en local con 4 cores

        // Step 2: create a Java Spark Context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //creo el manejador de spark

        // Step 3: initialize an array of integers
        Integer[] numbers = new Integer[]{1, 2, 3, 4, 5, 6, 7, 8};

        // Step 4: create a list of integers
        List<Integer> integerList = Arrays.asList(numbers);

        // Step 5: create a JavaRDD
        JavaRDD<Integer> distributedList = sparkContext.parallelize(integerList);
        //convierte la  lista en una lista distribuida de spark

        // Step 6: sum the numbers
        int sum;
        sum = distributedList.reduce((p1, p2) -> p1 + p2);
        //aplico una landa en la que a partir de una lista dejo un solo valor, en este caso devuelvo la suma

        // Step 6: print the sum
        System.out.println("The sum is: " + sum);

        // Step 7: stop the spark context
        sparkContext.stop();
    }
}
