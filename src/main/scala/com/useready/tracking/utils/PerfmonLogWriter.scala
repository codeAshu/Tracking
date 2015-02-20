package com.useready.tracking.utils


import java.io.{FileWriter, File}

import com.useready.tracking.RAMLog
import org.apache.spark.rdd.RDD
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat

/**
 * Created by Ashu on 17-02-2015.
 */
object PerfmonLogWriter {




  val totalCPU = 100 //percentage
  val totalRAM = 117000000000.00 //bytes           //TODO:populate these values from msinfo32file and make available
  val totalDisk = 11450000000000.00 //bytes

  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
  val path = "output/"                                          //global path for output


  /**
   * This function will create a RAM file as used available and total field,
   * similar to disk, CPU files.
   * @param ramLog
   */
  def createRAMFile(ramLog: RDD[RAMLog]): Unit =  {

    val logWritable = ramLog.map(line => Array(line.worker,line.dateTime,line.total,line.used,line.available)
      .mkString(",")).collect()

    //this will create a file each time
    val filename = (path+"RAMX")
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
    val filename = path+counter+"-"+flag+"-"+System.currentTimeMillis().toString + ".txt"
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

    Array(worker,dateTimeStr,totalCPU,used._2,avail, flag).mkString(",")

  }

  def diskLogWriter(used: (Double,Double), worker: String, interval: Int, flag: String, time: DateTime ) ={

    val dateTime = time.plusSeconds(used._1.toInt * interval)
    val dateTimeStr = dateTime.toString(fm)
    val avail = totalDisk - used._2

    Array(worker,dateTimeStr,totalDisk,used._2,avail, flag).mkString(",")

  }

  def ramLogWriter(used: (Double,Double), worker: String, interval: Int, flag: String, time: DateTime) = {

    val dateTime = time.plusSeconds(used._1.toInt * interval)
    val dateTimeStr = dateTime.toString(fm)
    val avail = totalRAM - used._2
    Array(worker,dateTimeStr,totalRAM,used._2,avail, flag).mkString(",")
  }

}
