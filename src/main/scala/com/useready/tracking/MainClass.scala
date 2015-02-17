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

    //cpuLogs
    val cpuLogs = sc.textFile("data/cpu.csv")
      .map(PerfmonLogs.parseCPULogLine)
      .filter(line => line.worker!="x")
      .cache()

    cpuLogs.foreach(println)
    //generate CPU logs by day, fortnight, month, year
    var prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","yearly")
    //prediction.foreach(println)
    var thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU?: "+thresholdCrossed)


    //diskLogs
    val diskLogs = sc.textFile("data/DISK.csv").map(PerfmonLogs.parseDiskLogLine)
      .filter(line => line.worker!="x")
      .cache()

    //generate Disk logs by day, fortnight, month, year
    diskLogs.foreach(println)
    prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","yearly")
    prediction.foreach(println)
    thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for Disk?: "+thresholdCrossed)


    //ramLogs
   val ramLogs = sc.textFile("data/RAM.csv").map(PerfmonLogs.parseRAMLogLine)
     .filter(line => line.worker!="x")
     .cache()

    ramLogs.foreach(println)
    prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","yearly")
    prediction.foreach(println)
    thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM?: "+thresholdCrossed)


  }

}
