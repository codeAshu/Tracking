package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by abhilasha on 10-02-2015.
 */
object GenerateDiskPredictions {

  def getPrediction(diskLogs: RDD[DiskLog], sc: SparkContext, extrapolationType: String, extrapolationDuration: String)
  : IndexedSeq[Double] = {

    //    val window = 10

    var prediction: IndexedSeq[Double] = null
    /*
    TODO :So based on the extrapolation window, decide the value of window
    to smooth the analysis
    day:window :
    fortnight :50
    month :100
    year :500
     */

    /*
        Year prediction
         */
    if (extrapolationDuration.equals("yearly")) {
      val labeledLogs = DataPreparationDisk.movingAverageOfDiskLogs(diskLogs.filter(line => line.dateTime.
        isAfter( DateTime.now.minusYears(1) )),25)
      //      val labeledLogs = DataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
      //        isAfter(DateTime.now.minusYears(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Month prediction
     */
    if(extrapolationDuration.equals("monthly"))
    {
      val labeledLogs = DataPreparationDisk.labeledPointRDDOfDiskLogs(diskLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusMonths(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    if(extrapolationDuration.equals("weekly"))
    {
      val labeledLogs = DataPreparationDisk.labeledPointRDDOfDiskLogs(diskLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    if(extrapolationDuration.equals("fortnightly"))
    {
      val labeledLogs = DataPreparationDisk.labeledPointRDDOfDiskLogs(diskLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(2))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    if(extrapolationDuration.equals("daily"))
    {
      val labeledLogs = DataPreparationDisk.labeledPointRDDOfDiskLogs(diskLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusDays(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }


    return prediction
  }

}
