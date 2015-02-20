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
                    extrapolationDuration: String,
                    time : DateTime) : IndexedSeq[(Double,Double)] = {

    var prediction: IndexedSeq[(Double,Double)] = null

    /*
    year prediction
     */
    if (extrapolationDuration.equals("Y")) {
      //val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogsMovingAverage(ramLog
      //  .filter(line => line.dateTime.
       // isAfter( DateTime.now.minusYears(1) )),25)
            val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
              isAfter(time.minusYears(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Month prediction
     */
    if(extrapolationDuration.equals("M"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(time.minusMonths(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    if(extrapolationDuration.equals("W"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(time.minusWeeks(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    if(extrapolationDuration.equals("F"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(time.minusWeeks(2))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    if(extrapolationDuration.equals("D"))
    {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogs(ramLog.filter(line => line.dateTime.
        isAfter(time.minusDays(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }
   prediction
  }
}