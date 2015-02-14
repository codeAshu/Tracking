package com.useready.tracking

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
Class for Disk performance logs
 */
case class DiskLog(worker: String, dateTime: DateTime,
                   total: Long, used: Long, available: Long ) {

}
/**
Class for CPU performance logs
 */
case class CPULog(worker: String, dateTime: DateTime,
                  total: Double , used: Double, available: Double ) {

}


/**
Class for RAM performance logs
 */
case class RAMLog(worker: String, dateTime: DateTime,
                  total: Double , totalActive : Double, dataserver : Double,
                   wgserver : Double,vizqlserver : Double,backgrounder: Double,
                   postgres : Double, deserver64: Double, otherProcess:Double ) {
}

object PerfmonLogs {
  /**
   * This function parse RAM perfmon file which has the file structure as
   * "worker" , "(PDH-CSV 4.0) (India Standard Time)(-330)", 	"\\PRIMARY\Process(_Total)\Private Bytes"
   * "Total Active"	, "\\PRIMARY\Process(dataserver)\Private Bytes",
   * "\\PRIMARY\Process(wgserver)\Private Bytes", 	"\\PRIMARY\Process(vizqlserver)\Private Bytes",
   * "\\PRIMARY\Process(backgrounder)\Private Bytes",	"\\PRIMARY\Process(postgres)\Private Bytes",
   * "\\PRIMARY\Process(tdeserver64)\Private Bytes",	"Other process/ Private bytes"
   * @param log : log line as string
   * @return  RAMLog class object
   */

  def parseRAMLogLine(log: String) : RAMLog = {
    val logVec = log.split(",").toVector
    val worker = logVec(0)

    //unquote the string
    val logVecUnq = logVec.map(w => w.replaceAll("^\"|\"$", ""))
    val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
    try {

      val strDateTime = logVec(1)
      val dateTime = fm.parseDateTime(strDateTime)

      //unquote the string
       RAMLog(worker, dateTime, logVecUnq(2).toDouble,logVecUnq(3).toDouble,
         logVecUnq(4).toDouble,logVecUnq(5).toDouble, logVecUnq(6).toDouble,
         logVecUnq(7).toDouble,logVecUnq(8).toDouble,logVecUnq(9).toDouble,
         logVecUnq(10).toDouble)

    }catch {
      //filter these later worker "x" is erroneous log line
      case e: Exception =>
        val fm = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss.SSS")
        val defaultTime =  fm.parseDateTime("01/01/1915 19:09:47.598")
        RAMLog("x",defaultTime,0,0,0,0,0,0,0,0,0)

    }
  }

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

    val fm = DateTimeFormat.forPattern("MM/dd/yy HH:mm:ss")
    val defaultTime =  fm.parseDateTime("01/01/15 19:09:47")

    val logVec = log.split(",").toVector

    println(logVec)
    try{

      var strDateTime = logVec(1).replaceAll("^\"|\"$", "")
      strDateTime = strDateTime.replaceAll("-"," ")

      val dateTime = fm.parseDateTime(strDateTime)



      //unquote the string
      var total:String = logVec(2).trim().replaceAll("^\"|\"$", "")

      var i:Int = 3
      var str = logVec(i).trim()
      while(str.charAt(str.length-1) != '\"')
      {
        total = total + str.replaceAll(",","")
        i = i+1
        str = logVec(i).trim()
      }
      total = total + logVec(i).trim().replaceAll("^\"|\"$", "")
      println(total+" total")
      i = i+1


      var used = logVec(i).trim().replaceAll("^\"|\"$", "")
      i = i+1
      str = logVec(i).trim()
      while(str.charAt(str.length-1) != '\"')
      {
        used = used + str.replaceAll(",","")
        i = i+1
        str = logVec(i).trim()
      }
      used = used + logVec(i).trim().replaceAll("^\"|\"$", "")
      println(used+" used")
      i = i+1

      var avail = logVec(i).trim().replaceAll("^\"|\"$", "")
      i = i+1
      str = logVec(i).trim()
      while(str.charAt(str.length-1) != '\"')
      {
        avail = avail + str.replaceAll(",","")
        i = i+1
        str = logVec(i).trim()
      }
      avail = avail + logVec(i).trim().replaceAll("^\"|\"$", "")
      println(avail+" avail")

      //return the class object
      DiskLog(logVec(0), dateTime, total.toLong, used.toLong, avail.toLong)

    }catch {
      case e: Exception =>
        println("error " + log) //DiskLog("w1","01/18/95-15:12:49",5000l,5000l,5000l)
        val fm = DateTimeFormat.forPattern("dd/MM/yy HH:mm:ss")
        val defaultTime =  fm.parseDateTime("01/01/15 19:09:47")
        DiskLog("w1",defaultTime,5000l,5000l,5000l)


    }
  }
}
