package org.spark.masterbigdata.dataframe;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DateFormat {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark program to work with dates")
                .master("local[8]")
                .getOrCreate();

        Logger.getLogger("").setLevel(Level.OFF);

        Dataset<Row> dataframe = sparkSession
                .read()
                .option("header", "true")
                .option("inferschema", "true")
                .option("delimiter", ",")
                .csv("./src/main/resources/Crimes_-_2019.csv");

        dataframe.printSchema();


        UDF1 stringToDate = new UDF1<String, Timestamp>() {
            public Timestamp call(final String str) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
                return new Timestamp(sdf.parse(str).getTime());
            }
        };

        sparkSession.udf().register("stringToDate", stringToDate, DataTypes.TimestampType);

        Calendar calendar = GregorianCalendar.getInstance();
        calendar.add(Calendar.MONTH, -3); // 3 MONTHS AGO
        long from = calendar.getTimeInMillis();

        dataframe
                .withColumn("Timestamp", functions.callUDF("stringToDate", functions.col("Date")))
                .filter(functions.col("Timestamp").gt(new Timestamp(from)))
                .show();

    }
}
