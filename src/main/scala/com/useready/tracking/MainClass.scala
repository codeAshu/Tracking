package com.useready.tracking

import org.apache.spark.SparkContext

/**
 * Created by abhilasha on 10-02-2015.
 */
object MainClass {

  def main(args: Array[String]) {

    //This is only added for windows 8.1, its a bug in Spark and this workaround is find here
    //http://apache-spark-user-list.1001560.n3.nabble.com/rdd-saveAsTextFile-problem-td176.html
    System.setProperty("hadoop.home.dir", "winutil\\")  //comment out for linux
    val sc = new SparkContext("local", "extrapolatio")

    //open the log files for reading
    GenerateAllPredictions.openLogFiles(sc)

    //generate all cpu predictions by day, week, fortnight, month and year
    GenerateAllPredictions.generateAllCPUPredictions(sc)

    //generate all disk predictions by day, week, fortnight, month and year
    GenerateAllPredictions.generateAllDiskPredictions(sc)

    //generate all ram predictions by day, week, fortnight, month and year
    GenerateAllPredictions.generateAllRAMPredictions(sc)

    //close open file pointers
    GenerateAllPredictions.clean()

  }

}
