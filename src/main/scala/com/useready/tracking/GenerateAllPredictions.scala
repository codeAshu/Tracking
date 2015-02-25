package com.useready.tracking

import java.io.{IOException, FileNotFoundException, File, PrintWriter}
import org.joda.time.DateTime
import utils._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object GenerateAllPredictions {

  var thresholdWriter = new PrintWriter("output/logfile.txt")
  val durationFlags = List("Y","M","F","W","D")

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
  def GeneratePredictionHof(sc: SparkContext,
          worker : String,
          interval :Int,
          algo :String,
          time :DateTime,
          counter: String,
          path:String,
          parser : String=> Log,
          fileCreater: RDD[Log] => Unit,
          logWriter : ((Double,Double), String, Int,String,DateTime) => String) ={

    try {

      //parse
      val diskLogs = sc.textFile(path).map(parser)
        .filter(line => line.worker != "x")
        .cache()

      //create file for real logs
      fileCreater(diskLogs)

      //generate prediction
      for (flag <- durationFlags) {

        val predictionPoint = GeneratePredictions.getPrediction(diskLogs, sc, algo, flag, time)

        if (predictionPoint != null) {

          val prediction = predictionPoint.map(used => logWriter(used, worker, interval, flag, time))

          //write the file
          PerfmonLogWriter.FileWriter(prediction, counter, flag)

          //write threshold info
          val thresholdCrossed = CheckThreshold.thresholdCrossed(predictionPoint, counter)
          println("Threshold crossed for " + counter + "  with " + flag + " data?: " + thresholdCrossed)
          thresholdWriter.write("Threshold crossed for " + counter + " with " + flag + " data?: " + thresholdCrossed + "\n")
        }
      }
    }catch {
      case ex: FileNotFoundException =>{
        println("Missing "+counter+" file")
      }
      case ex: IOException => {
        println("IO Exception for "+counter+" file")
      }
    }
  }

  def clean(): Unit ={
    //close the log file object
    thresholdWriter.close()
  }

}
