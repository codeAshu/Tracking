package com.useready.tracking

import org.apache.spark.SparkContext

/**
 * Created by abhilasha on 10-02-2015.
 */
object MainClass {

  def main(args: Array[String]) {

    //This is only added for windows 8.1, its a bug in Spark and this workaround is find here
    //http://apache-spark-user-list.1001560.n3.nabble.com/rdd-saveAsTextFile-problem-td176.html
    System.setProperty("hadoop.home.dir", "C:\\Users\\abhilasha\\hadoop-common-2.2.0-bin-master\\")  //comment out for linux

    val sc = new SparkContext("local", "extrapolation")

    val cpuLogs = sc.textFile("data/cpu.csv").map(PerfmonLogs.parseCPULogLine)
      .filter(line => line.worker!="x")
      .cache()

    //generate CPU logs by day, fortnight, month, year

    val prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","yearly")

    prediction.foreach(println)
  }

}