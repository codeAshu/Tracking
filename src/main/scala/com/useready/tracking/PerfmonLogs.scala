package com.useready.tracking

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


/*
Class for Disk performance logs
 */
case class DiskLog(worker: String, dateTime: DateTime,
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
