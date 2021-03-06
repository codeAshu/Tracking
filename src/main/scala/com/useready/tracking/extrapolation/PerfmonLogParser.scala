package com.useready.tracking

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/**
Class for Disk performance logs
flag meaning:
  R - real log
  D - Daily prediction
  W - Weekly prediction
  M - monthly prediction
  F - fortnight prediction
  Q - quarterly prediction
  Y - yearly prediction
 */

case class Log(worker: String, dateTime: DateTime,
                           total: Double, used: Double, available: Double, flag: String )

class DiskLog(worker: String, dateTime: DateTime,
              total: Long, used: Long, available: Long,
              flag: String ) extends Log(worker: String, dateTime: DateTime,
  total: Double, used: Double, available: Double, flag: String )
/**
Class for CPU performance logs
 */
class CPULog(worker: String, dateTime: DateTime,
             total: Double, used: Double, available:Double,
             flag: String ) extends Log(worker: String, dateTime: DateTime,
  total: Double, used: Double, available: Double, flag: String )


/**
Class for RAM performance logs with all processes
 */
case class RAMProcessLog(worker: String, dateTime: DateTime,
                  total: Double , totalActive : Double, dataserver : Double,
                   wgserver : Double,vizqlserver : Double,backgrounder: Double,
                   postgres : Double, deserver64: Double, otherProcess:Double ) {
}

class RAMLog(worker: String, dateTime: DateTime,
             total: Double, used: Double, available: Double,
             flag: String ) extends Log(worker: String, dateTime: DateTime,
  total: Double, used: Double, available: Double, flag: String )


object PerfmonLogs {

  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")
  val defaultTimeStr = "01/01/1915 19:09:47.598"

def parseRAMProcessLog(line: RAMProcessLog) :RAMLog ={
  try {
    val used = line.dataserver +line.wgserver + line.vizqlserver + line.backgrounder +
      line.postgres + line.deserver64 + line.otherProcess

    val available = RAM.total - used

   new RAMLog(line.worker, line.dateTime, line.total, used, available, "R")
  }
  catch {
    case e: Exception =>
      val defaultTime =  fm.parseDateTime(defaultTimeStr)
     new RAMLog("x",defaultTime,0.,0.,0.,"R")
  }
}


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

    try {
      val logVec = log.split(",").toVector
      val worker = logVec(0)
      //unquote the string
      val logVecUnq = logVec.map(w => w.replaceAll("^\"|\"$", ""))
      val strDateTime = logVec(1)
      val dateTime = fm.parseDateTime(strDateTime)

      //unquote the string
      parseRAMProcessLog( RAMProcessLog(worker, dateTime, logVecUnq(2).toDouble,logVecUnq(3).toDouble,
         logVecUnq(4).toDouble,logVecUnq(5).toDouble, logVecUnq(6).toDouble,
         logVecUnq(7).toDouble,logVecUnq(8).toDouble,logVecUnq(9).toDouble,
         logVecUnq(10).toDouble))
    }catch {
      //filter these later worker "x" is erroneous log line
      case e: Exception =>
        val defaultTime =  fm.parseDateTime(defaultTimeStr)
        parseRAMProcessLog(RAMProcessLog("x",defaultTime,0,0,0,0,0,0,0,0,0))

    }
  }

  /**
   * This function parse CPU perfmon file which has the file structure as
   * "worker","(PDH-CSV 4.0) (India Standard Time)(-330)","\\worker\Processor(_Total)\% Processor Time"s
   * @param log : log line as string
   * @return CPULog class object
   */
  def parseCPULogLine(log: String) : CPULog = {
    try {
      val logVec = log.split(",").toVector
      val strDateTime = logVec(1).replaceAll("^\"|\"$", "")
      val dateTime = fm.parseDateTime(strDateTime)

      //unquote the string
      val used = logVec(2).trim().replaceAll("^\"|\"$", "")
      val avail = 100.0 - used.toDouble

      //return the class object
     new CPULog(logVec(0), dateTime, 100.0, used.toDouble, avail, "R")

    }catch {
      //filter these later worker "x" is erroneous log line
      case e: Exception =>
        val defaultTime =  fm.parseDateTime(defaultTimeStr)
       new CPULog("x",defaultTime,0l,0l,0l,"R")

    }
  }

  /**
   *This function parse perfmon Disk data
   * @param log log line as string
   * @return  DiskLog Class object
   */
  def parseDiskLogLine(log: String): DiskLog = {

    try{
      val logVec = log.split(",").toVector
      var strDateTime = logVec(1).replaceAll("^\"|\"$", "")
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
     new DiskLog(logVec(0), dateTime, total.toLong, used.toLong, avail.toLong,"R")

    }catch {
      case e: Exception =>
        println("error " + log) //DiskLog("w1","01/18/95-15:12:49",5000l,5000l,5000l)
        val defaultTime =  fm.parseDateTime(defaultTimeStr)
       new DiskLog("x",defaultTime,5000l,5000l,5000l,"R")


    }
  }
}
