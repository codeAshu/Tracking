package com.useready.tracking.recommendation

import java.io.{PrintWriter, FileWriter, File, FilterWriter}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by Ashu on 10-03-2015.
 */
object StatsWriter {
  val format = new java.text.SimpleDateFormat("dd-MM-yyyy")
  val path =  "output/Recommendation/stats/"
  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")

  val cpuPath =  "output/Recommendation/stats/CPU/"                                          //global path for output
  val ramPath =  "output/Recommendation/stats/RAM/"
  val diskPath = "output/Recommendation/stats/DISK/"

  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
 }

  def FileWriter(statsRdd: RDD[String], counter: String,sc:SparkContext) = {
    var filename = ""

    counter match {
      case "CPU" =>  filename = cpuPath+counter+"X" + ".csv"
      case "DISK" => filename = diskPath+counter+"X" + ".csv"
      case "RAM" =>  filename = ramPath+counter+"X" + ".csv"
    }

    //collect the RDD
    val stats = statsRdd.collect()

  stats.map(p=>appendToFile(filename,p))
  }


  def allStatsCompiler(stats: (Int, Double),
                     worker : String,
                     period: String,
                     header : SimpleCSVHeader,
                     recommendationTime: DateTime ) ={


    val processName = header(stats._1+3)
    val processContri= stats._2
    Array(worker,recommendationTime,period,processName,processContri).mkString(",")

  }



  /**
   * This function is used to write the statistics of each counter by day, eve, morning, night
   * It writes as worker, dateTime, percent of each process...
   * @param stats
   * @param worker
   * @param period
   * @param header
   * @param counter
   * @param RecommendationTime
   * @param sc
   */
  def counterStatsWriter(stats: RDD[(Int, Double)],
                         worker : String,
                         period: String,
                         header : SimpleCSVHeader,
                         counter: String,
                         RecommendationTime: DateTime,
                         sc :SparkContext) ={
    var filename = ""
    counter match {
      case "CPU" =>  filename = cpuPath+worker+"-"+counter+"-"+period+"-"+format.format(new java.util.Date()) + ".csv"
      case "DISK" => filename = diskPath+worker+"-"+counter+"-"+period+"-"+format.format(new java.util.Date()) + ".csv"
      case "RAM" =>  filename = ramPath+worker+"-"+counter+"-"+period+"-"+format.format(new java.util.Date()) + ".csv"
    }

    //create header for the file
    val processIndex = stats.map(w=>w._1+3).collect()
    val headerString = header(0)+","+header(1)+",period,"+processIndex.map(w=>header(w)).mkString(",")

    //create stats to write
    val processValues = stats.map(w=>w._2*100).collect()
    val writableLine = worker+","+ RecommendationTime.toString(fm)+","+period+","+processValues.mkString(",")


    val outFile = new File(filename)
    printToFile(outFile) { p =>
      p.println(headerString)
      p.println(writableLine)
    }

  }


}
