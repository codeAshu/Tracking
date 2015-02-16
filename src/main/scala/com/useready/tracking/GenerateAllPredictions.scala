package com.useready.tracking

import java.io.{File, PrintWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Created by abhilasha on 14-02-2015.
 */
object GenerateAllPredictions {

  var cpuLogs: RDD[CPULog] = null;
  var diskLogs: RDD[DiskLog] = null
  var ramLogs: RDD[RAMLog] = null;
  var logWriter = new PrintWriter("output/logfile.txt")

  def openLogFiles(sc: SparkContext): Unit =
  {
    /*
    Open the CPU logs
     */
    cpuLogs = sc.textFile("data/cpu.csv")
      .map(PerfmonLogs.parseCPULogLine)
      .filter(line => line.worker!="x")
      .cache()

    /*
    Open the Disk logs
     */
    diskLogs = sc.textFile("data/DISK.csv").map(PerfmonLogs.parseDiskLogLine)
          .filter(line => line.worker!="x")
          .cache()

    /*
    Open the RAM logs
     */
    ramLogs = sc.textFile("data/RAM.csv").map(PerfmonLogs.parseRAMLogLine)
        .filter(line => line.worker!="x")
        .cache()
  }

  /**
   * generate CPU logs by day, week, fortnight, month, year
   * @param sc
   * @return
   */
  def generateAllCPUPredictions(sc: SparkContext): Unit ={

    //yearly prediction
    var prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","yearly")
    val yearFile = new File("output/CPU-yearly-"+System.currentTimeMillis().toString+".txt")
    var writer = new PrintWriter(yearFile)
    writer.write(prediction.toString)
    writer.close()
    var thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU with yearly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for CPU with yearly data?: "+thresholdCrossed+"\n")

    //monthly prediction
    prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","monthly")
    val monthFile = new File("output/CPU-monthly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(monthFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU with monthly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for CPU with monthly data?: "+thresholdCrossed+"\n")

    //weekly prediction
    prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","weekly")
    val weekFile = new File("output/CPU-weekly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(weekFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU with weekly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for CPU with weekly data?: "+thresholdCrossed+"\n")

    //fortnightly prediction
    prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","fortnightly")
    val fortnightFile = new File("output/CPU-fortnightly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(fortnightFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU with fortnightly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for CPU with fortnightly data?: "+thresholdCrossed+"\n")

    //daily prediction
    prediction = GenerateCPUPredictions.getPrediction(cpuLogs, sc, "linear","daily")
    val dayFile = new File("output/CPU-daily-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(dayFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.cpuThresholdCrossed(prediction)
    println("Threshold crossed for CPU with daily data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for CPU with daily data?: "+thresholdCrossed+"\n")


  }

  /**
   * generate Disk logs by day, week, fortnight, month, year
   * @param sc
   * @return
   */
  def generateAllDiskPredictions(sc: SparkContext): Unit ={

    //yearly prediction
    var prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","yearly")
    val yearFile = new File("output/Disk-yearly-"+System.currentTimeMillis().toString+".txt")
    var writer = new PrintWriter(yearFile)
    writer.write(prediction.toString)
    writer.close()
    var thresholdCrossed  = CheckThreshold.diskThresholdCrossed(prediction)
    println("Threshold crossed for Disk with yearly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for Disk with yearly data?: "+thresholdCrossed+"\n")

    //monthly prediction
    prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","monthly")
    val monthFile = new File("output/Disk-monthly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(monthFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.diskThresholdCrossed(prediction)
    println("Threshold crossed for Disk with monthly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for Disk with monthly data?: "+thresholdCrossed+"\n")

    //weekly prediction
    prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","weekly")
    val weekFile = new File("output/Disk-weekly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(weekFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.diskThresholdCrossed(prediction)
    println("Threshold crossed for Disk with weekly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for Disk with weekly data?: "+thresholdCrossed+"\n")

    //fortnightly prediction
    prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","fortnightly")
    val fortnightFile = new File("output/Disk-fortnightly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(fortnightFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.diskThresholdCrossed(prediction)
    println("Threshold crossed for Disk with fortnightly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for Disk with fortnightly data?: "+thresholdCrossed+"\n")

    //daily prediction
    prediction = GenerateDiskPredictions.getPrediction(diskLogs, sc, "linear","daily")
    val dayFile = new File("output/Disk-daily-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(dayFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.diskThresholdCrossed(prediction)
    println("Threshold crossed for Disk with daily data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for Disk with daily data?: "+thresholdCrossed+"\n")

  }

  /**
   * generate RAM logs by day, week, fortnight, month, year
   * @param sc
   * @return
   */
  def generateAllRAMPredictions(sc: SparkContext): Unit ={

    //yearly prediction
    var prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","yearly")
    val yearFile = new File("output/RAM-yearly-"+System.currentTimeMillis().toString+".txt")
    var writer = new PrintWriter(yearFile)
    writer.write(prediction.toString)
    writer.close()
    var thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM with yearly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for RAM with yearly data?: "+thresholdCrossed+"\n")

    //monthly prediction
    prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","monthly")
    val monthFile = new File("output/RAM-monthly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(monthFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM with monthly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for RAM with monthly data?: "+thresholdCrossed+"\n")

    //weekly prediction
    prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","weekly")
    val weekFile = new File("output/RAM-weekly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(weekFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM with weekly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for RAM with weekly data?: "+thresholdCrossed+"\n")

    //fortnightly prediction
    prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","fortnightly")
    val fortnightFile = new File("output/RAM-fortnightly-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(fortnightFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM with fortnightly data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for RAM with fortnightly data?: "+thresholdCrossed+"\n")

    //daily prediction
    prediction = GenerateRAMPredictions.getPrediction(ramLogs, sc, "linear","daily")
    val dayFile = new File("output/RAM-daily-"+System.currentTimeMillis().toString+".txt")
    writer = new PrintWriter(dayFile)
    writer.write(prediction.toString)
    writer.close()
    thresholdCrossed  = CheckThreshold.ramThresholdCrossed(prediction)
    println("Threshold crossed for RAM with daily data?: "+thresholdCrossed)
    logWriter.write("Threshold crossed for RAM with daily data?: "+thresholdCrossed+"\n")
  }

  def clean(): Unit ={
    logWriter.close()
  }

}
