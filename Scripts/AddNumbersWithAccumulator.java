package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * Program that sums a list of numbers using Apache Spark
 *
 * @author Antonio J. Nebro
 */
public class AddNumbersWithAccumulator {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf()
                .setAppName("Accumulators")
                .setMaster("local[8]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<Integer> integerList = Arrays.asList(new Integer[]{1, 2, 3, 4, 5, 6, 7, 8});

        LongAccumulator accum = sparkContext.sc().longAccumulator();

        int sum = sparkContext
                .parallelize(integerList)
                .reduce((integer, integer2) -> {
                    accum.add(1);
                    return integer + integer2;
                });

        System.out.println("The sum is: " + sum);
        System.out.println("Number of reduces: " + accum.value());

        // Step 7: stop the spark context
        sparkContext.stop();
    }
}
