package com.useready.tracking.recommendation

import com.useready.tracking.CheckThreshold
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// Import Spark SQL data types and Row.
import org.apache.spark.sql._
/**
 * Created by Ashu on 05-03-2015.
 */

object MainClassRecom {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "winutil\\")  //comment out for linux
    val sc = new SparkContext("local", "recom")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD //TODO: create data file
//    val diskData = sc.textFile("data/disk_process.csv")
//    val cpuData = sc.textFile("data/cpu_process.csv")


    //currently running with only RAM data for two workers
    val ramData1 = sc.textFile("data/w1-process.csv")
    val ramData2 = sc.textFile("data/w2-process.csv")

    val recommendationTime =  DateTime.now()

    //DataParser.Parsedata(cpuData,recommendationTime,sc,sqlContext)

    DataParser.Parsedata(ramData1,recommendationTime,sc,sqlContext)
    DataParser.Parsedata(ramData2,recommendationTime,sc,sqlContext)

    //DataParser.Parsedata(diskData,recommendationTime,sc,sqlContext)

  }

}
