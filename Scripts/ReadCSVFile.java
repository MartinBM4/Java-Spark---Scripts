package org.masterbigdata.spark;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import static java.lang.Integer.valueOf;

/**
 * Program that sums mumbers contained in files using Apache Spark
 * @author Antonio J. Nebro
 */
public class ReadCSVFile {
  //  QUITAR EL COMENTARIOOOOOOOOO:
  //  static Logger log = Logger.getLogger(CrimeAnalysis.class.getName());

  public static <S> void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.OFF) ;

    // Step 1: create a SparkConf object
    SparkConf conf = new SparkConf()
            .setAppName("Read CSV file")
            .setMaster("local[4]") ;

    // Step 2: create a Java Spark Context
    JavaSparkContext sparkContext = new JavaSparkContext(conf) ;

    /* Step 3: read the data and perform de sum */
    JavaRDD<String> lines = sparkContext.textFile("data/Film_Locations_in_San_Francisco.csv") ;

    JavaRDD<String[]> fields = lines.map(line -> line.split(",")) ;

    JavaRDD<String> filmNames = fields.map(array -> array[0]) ;

    List<String> result = filmNames
            .sortBy(s -> s, false, 1)
            .take(20) ;

    result.stream().forEach(System.out::println) ;

    sparkContext.stop() ;
  }
}
