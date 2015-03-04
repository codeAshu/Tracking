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

  def main(args: Array[String]) {
    //This is only added for windows 8.1, its a bug in Spark and this workaround is find here
    //http://apache-spark-user-list.1001560.n3.nabble.com/rdd-saveAsTextFile-problem-td176.html
    System.setProperty("hadoop.home.dir", "winutil\\")  //comment out for linux
    val sc = new SparkContext("local", "extrapolation")

    //decide time, worker, algo, to run the prediction
    //write all modifiable parameters here, which may also be exposed to user
    val diskPath = "data/DISK.csv"
    val ramPath = "data/RAM.csv"
    val cpuPath = "data/CPU.csv"

    val time =  DateTime.now()
    val worker = "w1"
    val algo = "ridge"
    val interval = 1  //interval at which logs are collected


    //generate all cpu predictions by day, week, fortnight, month and year for a worker
    GenerateAllPredictions.GeneratePredictionHof(sc,worker,interval,algo,time,CPU.name,cpuPath,
    CPU.parser,CPU.fileCreater,CPU.logWriter)

    GenerateAllPredictions.GeneratePredictionHof(sc,worker,interval,algo,time,RAM.name,ramPath,
      RAM.parser,RAM.fileCreater,RAM.logWriter)

    GenerateAllPredictions.GeneratePredictionHof(sc,worker,interval,algo,time,DISK.name,diskPath,
      DISK.parser,DISK.fileCreater,DISK.logWriter)

    //close open file pointers
    GenerateAllPredictions.clean()



  }

}
