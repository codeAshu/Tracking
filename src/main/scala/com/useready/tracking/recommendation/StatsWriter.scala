package com.useready.tracking.recommendation

import java.io.File

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

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  /**
   * This function is used to write the statistics of each counter by day, eve, morning, night
   * It writes as worker, dateTime, percent of each process...
   * @param stats
   * @param worker
   * @param duration
   * @param header
   * @param counter
   * @param RecommendationTime
   * @param sc
   */
  def counterStatsWriter(stats: RDD[(Int, Double)],
                         worker : String,
                         duration: String,
                         header : SimpleCSVHeader,
                         counter: String,
                         RecommendationTime: DateTime,
                         sc :SparkContext) ={
    var filename = ""
    counter match {
      case "CPU" =>  filename = path+counter+"-"+duration+"-"+format.format(new java.util.Date()) + ".csv"
      case "DISK" => filename = path+counter+"-"+duration+"-"+format.format(new java.util.Date()) + ".csv"
      case "RAM" =>  filename = path+counter+"-"+duration+"-"+format.format(new java.util.Date()) + ".csv"
    }

    //create header for the file
    val processIndex = stats.map(w=>w._1+3).collect()
    val headerString = header(0)+","+header(1)+","+processIndex.map(w=>header(w)).mkString(",")

    //create stats to write
    val processValues = stats.map(w=>w._2*100).collect()
    val writableLine = worker+","+ RecommendationTime.toString(fm)+","+processValues.mkString(",")


    val outFile = new File(filename)
    printToFile(outFile) { p =>
      p.println(headerString)
      p.println(writableLine)
    }
    
  }
  

}
