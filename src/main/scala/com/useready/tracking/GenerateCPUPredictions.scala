package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by abhilasha on 10-02-2015.
 */
object GenerateCPUPredictions {
  /**
   * This function calls the data preparation function and extrapolation algorithm for the time duration
   * given by extrapolationDuration
   * @param cpuLogs RDD of class CpuLogs
   * @param sc Spark Context
   * @param extrapolationType type of algorithm
   * @param extrapolationDuration duration of the extrapolation
   * @return
   */
  def getPrediction(cpuLogs: RDD[CPULog],
                    sc: SparkContext,
                    extrapolationType: String,
                    extrapolationDuration: String): IndexedSeq[Double] = {

    var prediction: IndexedSeq[Double] = null
    /*
    TODO :So based on the extrapolation window, decide the value of window to smooth the analysis
    day:window :
    fortnight :50
    month :100
    year :500
     */

   //here we have choice of either using moving avg or not choosig moving avg
    if (extrapolationDuration.equals("yearly")) {
        val labeledLogs = DataPreparationCPU.labeledPointRDDOfCPULogsMovingAverage(cpuLogs.filter(line => line.dateTime.
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
      val labeledLogs = DataPreparationCPU.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusMonths(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    if(extrapolationDuration.equals("weekly"))
    {
      val labeledLogs = DataPreparationCPU.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    if(extrapolationDuration.equals("fortnightly"))
    {
      val labeledLogs = DataPreparationCPU.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(2))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    if(extrapolationDuration.equals("daily"))
    {
      val labeledLogs = DataPreparationCPU.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusDays(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

   prediction
  }

}
