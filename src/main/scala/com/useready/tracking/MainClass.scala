package com.useready.tracking

import utils._
import org.apache.spark.SparkContext

import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat
import java.io._
/**
 * Created by abhilasha on 10-02-2015.
 */
object MainClass {

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }
  def main(args: Array[String]) {



    //This is only added for windows 8.1, its a bug in Spark and this workaround is find here
    //http://apache-spark-user-list.1001560.n3.nabble.com/rdd-saveAsTextFile-problem-td176.html
    System.setProperty("hadoop.home.dir", "winutil\\")  //comment out for linux
   val sc = new SparkContext("local", "extrapolatio")


    //generate all cpu predictions by day, week, fortnight, month and year

    //decide time, worker, algo, to run the prediction
    //write all modifiable parameters here, which may also be exposed to user
    val time =  DateTime.now()
    val worker = "w1"
    val algo = "linear"
    val interval = 2  //interval at which logs are collected

    GenerateAllPredictions.generateAllCPUPredictions(sc,worker,interval,algo,time)

    //generate all disk predictions by day, week, fortnight, month and year
    GenerateAllPredictions.generateAllDiskPredictions(sc,worker,interval,algo,time)

    //generate all ram predictions by day, week, fortnight, month and year
    GenerateAllPredictions.generateAllRAMPredictions(sc,worker,interval,algo,time)

    //close open file pointers
    GenerateAllPredictions.clean()



  }

}
