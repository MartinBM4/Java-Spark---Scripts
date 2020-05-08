package org.masterinformatica.sparkmachinelearning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.util.List;

/**
 * Created by ajnebro on 10/11/16.
 */
public class ReadLibSVMFileRDD {
  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.OFF);

    SparkConf sparkConf = new SparkConf()
            .setAppName("Read LibSVM file with RDDs")
            .setMaster("local[2]");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

    JavaRDD<LabeledPoint> points = MLUtils.loadLibSVMFile(sparkContext.sc(), "data/classificationDataLibsvm.txt").toJavaRDD() ;

    List<LabeledPoint> list = points.take(20) ;
    list.forEach(System.out::println);
  }
}
