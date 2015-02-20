package com.useready.tracking

import java.io.{File, PrintWriter}
import org.joda.time.DateTime
import utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GenerateAllPredictions {

  var logWriter = new PrintWriter("output/logfile.txt")
  val durationFlags = List("Y","M","F","W","D")

  /**
   *
   * @param f
   * @param op
   * @return
   */
  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * generate CPU logs by day, week, fortnight, month, year
   * @param sc
   * @return
   */
  def generateAllCPUPredictions(sc: SparkContext,
                                worker : String,
                                interval :Int,
                                 algo :String,
                                 time :DateTime): Unit = {

    val cpuLogs = sc.textFile("data/CPU.csv")
      .map(PerfmonLogs.parseCPULogLine)
      .filter(line => line.worker != "x")
      .cache()

    for (flag <- durationFlags) {

      val predictionPoint = GenerateCPUPredictions
        .getPrediction(cpuLogs, sc, algo, flag,time)

      if(predictionPoint != null) {
        val prediction = predictionPoint.map(used => PerfmonLogWriter
          .cpuLogWriter(used, worker, interval, flag, time))

        PerfmonLogWriter.FileWriter(prediction, "CPU", flag)

        val thresholdCrossed = CheckThreshold.thresholdCrossed(predictionPoint, "CPU")
        println("Threshold crossed for CPU with "+flag+" data?: " + thresholdCrossed)
        logWriter.write("Threshold crossed for CPU with "+flag+" data?: " + thresholdCrossed + "\n")
      }
    }
  }
    def generateAllDiskPredictions(sc: SparkContext,
                                  worker : String,
                                  interval :Int,
                                  algo :String,
                                  time :DateTime): Unit = {

      val diskLogs = sc.textFile("data/DISK.csv").map(PerfmonLogs.parseDiskLogLine)
        .filter(line => line.worker!="x")
        .cache()

      for (flag <- durationFlags) {

        val predictionPoint = GenerateDiskPredictions
          .getPrediction(diskLogs, sc, algo, flag, time)

        if(predictionPoint != null) {
          val prediction = predictionPoint.map(used => PerfmonLogWriter
            .diskLogWriter(used, worker, interval, flag, time))

          PerfmonLogWriter.FileWriter(prediction, "DISK", flag)

          val thresholdCrossed = CheckThreshold.thresholdCrossed(predictionPoint, "DISK")
          println("Threshold crossed for DISK with "+flag+" data?: " + thresholdCrossed)
          logWriter.write("Threshold crossed for DISK with " +flag+" data?: " + thresholdCrossed + "\n")
        }
      }
    }

  def generateAllRAMPredictions(sc: SparkContext,
                                 worker : String,
                                 interval :Int,
                                 algo :String,
                                 time :DateTime): Unit = {

    //coverted to class RAMLog
    val ramLogs = sc.textFile("data/RAM.csv")
      .map(PerfmonLogs.parseRAMLogLine)
      .map(PerfmonLogs.parseRAMProcessLog)
      .filter(line => line.worker != "x")
      .cache()

    //save a structure of RAM data as [Worker, DateTime, Total, Used, Available, Flag]
    PerfmonLogWriter.createRAMFile(ramLogs)

    for (flag <- durationFlags) {

      val predictionPoint = GenerateRAMPredictions
        .getPrediction(ramLogs, sc, algo, flag, time)

      if(predictionPoint != null) {
        val prediction = predictionPoint.map(used => PerfmonLogWriter
          .ramLogWriter(used, worker, interval, flag, time))

        PerfmonLogWriter.FileWriter(prediction, "RAM", flag)

        val thresholdCrossed = CheckThreshold.thresholdCrossed(predictionPoint, "RAM")
        println("Threshold crossed for RAM with " +flag+" data?: " + thresholdCrossed)
        logWriter.write("Threshold crossed for RAM with " +flag+" data?: " + thresholdCrossed + "\n")
      }
    }
  }
    def clean(): Unit ={
    //close the log file object
    logWriter.close()
  }

}
