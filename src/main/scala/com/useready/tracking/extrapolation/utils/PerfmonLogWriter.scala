package com.useready.tracking.utils


import java.io.{FileWriter, File}

import com.useready.tracking._
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat

/**
 * Created by Ashu on 17-02-2015.
 */
object PerfmonLogWriter {

  val totalCPU = 100 //percentage
  val totalRAM = RAM.total //bytes           //TODO:populate these values from msinfo32file and make available
  val totalDisk = DISK.total //bytes
  val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
  val cpuPath =  "output/Extrapolation/CPU/"                                          //global path for output
  val ramPath =  "output/Extrapolation/RAM/"
  val diskPath = "output/Extrapolation/DISK/"

  def createDiskFile(diskLog: RDD[Log]) = {
    val logWritable = diskLog.map(line => Array(line.worker,line.dateTime.toString(fm),line.total.toLong,
      line.used.toLong,
      line.available.toLong,line.flag)
      .mkString(",")).collect()

    val filename = (diskPath+"DISKX.csv")
    val outFile = new File(filename)
    printToFile(outFile) { p =>
      logWritable.foreach(p.println)
    }

  }
  def createCPUFile(cpuLog: RDD[Log]):Unit = {
    val logWritable = cpuLog.map(line => Array(line.worker,line.dateTime.toString(fm),line.total,line.used,
      line.available,line.flag)
      .mkString(",")).collect()

    val filename = (cpuPath+"CPUX.csv")
    val outFile = new File(filename)
    printToFile(outFile) { p =>
      logWritable.foreach(p.println)
    }
  }

  /**
   * This function will create a RAM file as used available and total field,
   * similar to disk, CPU files.
   * @param ramLog
   */
  def createRAMFile(ramLog: RDD[Log]): Unit =  {

    val logWritable = ramLog.map(line => Array(line.worker,line.dateTime.toString(fm),line.total,line.used,
      line.available,line.flag)
      .mkString(",")).collect()

    //this will create a file each duration
    val filename = (ramPath+"RAMX.csv")
    val outFile = new File(filename)
    printToFile(outFile) { p =>
      logWritable.foreach(p.println)
    }
    /* this code is for appending a file-but that is difficult
    val fw = new FileWriter("RAMX.csv", true)
    try {
      for (line <- logWritable) {
        fw.write(line+'\n')
      }
    }
    finally fw.close()
  */
  }

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
   *
   * @param prediction
   * @param counter
   * @param flag
   */
  def FileWriter(prediction: IndexedSeq[String], counter: String, flag: String) = {
    var filename = ""
    counter match {
      case "CPU" =>  filename = cpuPath+counter+"-"+flag+"-"+format.format(new java.util.Date()) + ".csv"
      case "DISK" => filename = diskPath+counter+"-"+flag+"-"+format.format(new java.util.Date()) + ".csv"
      case "RAM" =>  filename = ramPath+counter+"-"+flag+"-"+format.format(new java.util.Date()) + ".csv"
    }
    val outFile = new File(filename)
    printToFile(outFile) { p =>
      prediction.foreach(p.println)
    }
  }



  /**
   *
   * @param used
   * @param worker
   * @param interval
   * @return
   */
  def cpuLogWriter(used: (Double,Double), worker: String, interval: Int, flag: String, time: DateTime ) ={

    val dateTime = time.plusSeconds(used._1.toInt * interval)
    val dateTimeStr = dateTime.toString(fm)
    val avail = totalCPU - used._2
    if(avail>0)
      Array(worker,dateTimeStr,totalCPU,used._2,avail, flag).mkString(",")
    else
      Array(worker,dateTimeStr,totalCPU,used._2,0., flag).mkString(",")
  }

  def diskLogWriter(used: (Double,Double), worker: String, interval: Int, flag: String, time: DateTime ) ={

    val dateTime = time.plusSeconds(used._1.toInt * interval)
    val dateTimeStr = dateTime.toString(fm)
    val avail = totalDisk - used._2
    if(avail>0)
      Array(worker,dateTimeStr,totalDisk,used._2.toLong,avail.toLong, flag).mkString(",")
    else
      Array(worker,dateTimeStr,totalDisk,used._2.toLong,0l, flag).mkString(",")
  }

  def ramLogWriter(used: (Double,Double), worker: String, interval: Int, flag: String, time: DateTime) = {

    val dateTime = time.plusSeconds(used._1.toInt * interval)
    val dateTimeStr = dateTime.toString(fm)
    val avail = totalRAM - used._2
    if(avail>0)
      Array(worker,dateTimeStr,totalRAM,used._2,avail, flag).mkString(",")
    else
      Array(worker,dateTimeStr,totalRAM,used._2,0, flag).mkString(",")
  }

}
