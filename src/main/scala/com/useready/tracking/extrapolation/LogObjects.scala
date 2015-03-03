package com.useready.tracking

import com.useready.tracking.utils.PerfmonLogWriter

/**
 * Created by Ashu on 24-02-2015.
 */

//TODO: get thresholds and total from msinfo32
object CPU {


  //"CPU",sc,worker,interval,algo,time,cpuPath,
  //  PerfmonLogs.parseCPULogLine,PerfmonLogWriter.createCPUFile,
  //  GeneratePredictions.getPrediction,PerfmonLogWriter.cpuLogWriter
  val name = "CPU"
  val threshold = 80.0 //percent
  val parser = PerfmonLogs.parseCPULogLine(_)
  val fileCreater = PerfmonLogWriter.createCPUFile(_)
  val logWriter = PerfmonLogWriter.cpuLogWriter(_,_,_,_,_)
  val extrpolateStepSize: Double = 0.001
}

object RAM {


  //"CPU",sc,worker,interval,algo,time,cpuPath,
  //  PerfmonLogs.parseCPULogLine,PerfmonLogWriter.createCPUFile,
  //  GeneratePredictions.getPrediction,PerfmonLogWriter.cpuLogWriter

  val name = "RAM"
  val threshold = 80.0 //percent
  val total =  8000000000.00 //bytes
  val parser = PerfmonLogs.parseRAMLogLine(_)
  val fileCreater = PerfmonLogWriter.createRAMFile(_)
  val logWriter = PerfmonLogWriter.ramLogWriter(_,_,_,_,_)
  val extrpolateStepSize: Double = 0.001
}

object DISK {


  //"CPU",sc,worker,interval,algo,time,cpuPath,
  //  PerfmonLogs.parseCPULogLine,PerfmonLogWriter.createCPUFile,
  //  GeneratePredictions.getPrediction,PerfmonLogWriter.cpuLogWriter

  val name = "DISK"
  val threshold = 80.0 //percent
  val total =  485445595136.00 //bytes
  val parser = PerfmonLogs.parseDiskLogLine(_)
  val fileCreater = PerfmonLogWriter.createDiskFile(_)
  val logWriter = PerfmonLogWriter.diskLogWriter(_,_,_,_,_)
  val extrpolateStepSize: Double = 0.00002

}