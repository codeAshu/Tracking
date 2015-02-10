package com.useready.tracking

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/*
Class for Disk performance logs
 */
case class DiskLog(worker: String, dateTime: String,
                   total: Long, used: Long, available: Long ) {

}
/*
Class for CPU performance logs
 */
case class CPULog(worker: String, dateTime: DateTime,
                  total: Double , used: Double, available: Double ) {

}


/*
Class for RAM performance logs
 */
case class RAMLog(worker: String, dateTime: DateTime,
                  total: Double , used: Double, available: Double ) {

}


object PerfmonLogs {
  /**
   * This function parse CPU perfmon file which has the file structure as
   * "worker","(PDH-CSV 4.0) (India Standard Time)(-330)","\\worker\Processor(_Total)\% Processor Time"s
   * @param log : log line as string
   * @return CPULog class object
   */
  def parseCPULogLine(log: String) : CPULog = {

    val logVec = log.split(",").toVector
    val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
    try {

      val strDateTime = logVec(1).replaceAll("^\"|\"$", "")
      val dateTime = fm.parseDateTime(strDateTime)

      //unquote the string
      val used = logVec(2).trim().replaceAll("^\"|\"$", "")
      val avail = 100.0 - used.toDouble

      //return the class object
      CPULog(logVec(0), dateTime, 100.0, used.toDouble, avail)

    }catch {
      //filter these later worker "x" is erroneous log line
      case e: Exception =>
        val fm = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss.SSS")
        val defaultTime =  fm.parseDateTime("01/01/1915 19:09:47.598")
        CPULog("x",defaultTime,0l,0l,0l)

    }
  }

  /**
   *This function parse perfmon Disk data
   * @param log log line as string
   * @return  DiskLog Class object
   */
  def parseDiskLogLine(log: String): DiskLog = {
    try{


      if (1>2) {  //exception
        throw new RuntimeException("Cannot parse log line: " + log)
      }

      DiskLog("w1","01/18/95-15:12:49",0l,0l,0l)

    }catch {
      case e: Exception =>
        println("error " + log) //DiskLog("w1","01/18/95-15:12:49",5000l,5000l,5000l)
        DiskLog("w1","01/18/95-15:12:49",0l,0l,0l)

    }
  }
}
