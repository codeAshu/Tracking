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

    if (extrapolationDuration.equals("yearly")) {
      val labeledLogs = DataPreparationRAM.labeledPointRDDOfRAMLogsMovingAverage(ramLog
        .filter(line => line.dateTime.
        isAfter( DateTime.now.minusYears(1) )),25)
      //      val labeledLogs = DataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
      //        isAfter(DateTime.now.minusYears(1))))

      prediction = extrapolation.extrapolateLogs(labeledLogs, sc, extrapolationType)
    }

    //similarly add for others also


    prediction
  }
}