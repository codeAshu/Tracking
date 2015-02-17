package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by Ashu on 13-02-2015.
 */
object GenerateRAMPredictions {

  def getPrediction(ramLog: RDD[RAMLog],
                    sc: SparkContext,
                    extrapolationType: String,
                    extrapolationDuration: String) : IndexedSeq[Double] = {

    var prediction: IndexedSeq[Double] = null

    /*
    year prediction
     */
    if (extrapolationDuration.equals("yearly")) {
      //val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogsMovingAverage(ramLog
      //  .filter(line => line.dateTime.
       // isAfter( DateTime.now.minusYears(1) )),25)
            val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
              isAfter(DateTime.now.minusYears(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Month prediction
     */
    if(extrapolationDuration.equals("monthly"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(DateTime.now.minusMonths(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    if(extrapolationDuration.equals("weekly"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    if(extrapolationDuration.equals("fortnightly"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(2))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    if(extrapolationDuration.equals("daily"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(DateTime.now.minusDays(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }



    prediction
  }
}