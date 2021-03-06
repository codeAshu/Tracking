package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

object GeneratePredictions {
  /**
   * This function calls the data preparation function and extrapolation algorithm for the period period
   * given by extrapolationDuration
   * @param Logs RDD of class CpuLogs
   * @param sc Spark Context
   * @param extrapolationType type of algorithm
   * @param extrapolationDuration period of the extrapolation
   * @return
   */
  def getPrediction(counter:String,
                    Logs: RDD[Log],
                    sc: SparkContext,
                    extrapolationType: String,
                    extrapolationDuration: String,
                    time: DateTime):  IndexedSeq[(Double,Double)] = {

    var prediction: IndexedSeq[(Double,Double)] = null
    /*
    TODO :So based on the extrapolation window, decide the value of window to smooth the analysis
    day:window :
    fortnight :50
    month :100
    year :500
     */

   //here we have choice of either using moving avg or not choosig moving avg
    if (extrapolationDuration.equals("Y")) {
//        val labeledLogs = DataPreparationCPU.labeledPointRDDMovingAverageOfCPULogsMovingAverage(cpuLogs.filter(line => line.dateTime.
//         isAfter( DateTime.now.minusYears(1) )),25)

//      val labeledLogs = DataPreparation.labeledPointRDDMovingAverage(Logs.filter(line => line.dateTime.
//        isAfter(period.minusYears(1))),25)

      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
       isAfter(time.minusYears(1))))

      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }

   /*
  Quarter prediction
   */
    else if(extrapolationDuration.equals("Q"))
    {
      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
        isAfter(time.minusMonths(3))))

      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }
    /*
    Month prediction
     */
    else if(extrapolationDuration.equals("M"))
    {
      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
        isAfter(time.minusMonths(1)))
      )

      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    else if(extrapolationDuration.equals("W"))
    {
      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
        isAfter(time.minusWeeks(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    else if(extrapolationDuration.equals("F"))
    {
      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
        isAfter(time.minusWeeks(2))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    else if(extrapolationDuration.equals("D"))
    {
      val labeledLogs = DataPreparation.labeledPointRDD(Logs.filter(line => line.dateTime.
        isAfter(time.minusDays(1))))
      if(labeledLogs.count() != 0)
        prediction = extrapolation.extrapolateLogs(counter,labeledLogs, sc, extrapolationType)
    }

   prediction
  }

}
