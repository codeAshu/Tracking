package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * Created by abhilasha on 10-02-2015.
 */
object GenerateCPUPredictions {
  /**
   * This function takes the labeled point RDD and call the extrapolation function
   * based on extrapolation type
   * @param logs RDD of labeled point
   * @param sc Spark context
   * @param extrapolationType type of algorithm to choose for extrapolation
   * @return
   */
  def extrapolateCPULogs(logs: RDD[LabeledPoint],
                         sc: SparkContext,
                         extrapolationType: String) : IndexedSeq[Double] = {

    //count of cpu logs
    val nCpuLogs = logs.count()

    //create extrapolation independent variable list
    val testData = (nCpuLogs to 2 * nCpuLogs).map(x => x.toDouble)

    //call the function to extrapolate
    var model: GeneralizedLinearModel = null

    if(extrapolationType.equals("linear")) {
      model = extrapolation.extrapolateLinear(logs, sc)
    }

    if(extrapolationType.equals("ridge")) {
      model = extrapolation.extrapolateRidge(logs, sc)
    }

    if(extrapolationType.equals("lasso")) {
      model = extrapolation.extrapolateLasso(logs, sc)
    }

    //    val model = extrapolate(logs, sc)
    println("regression model: " +model)

    val prediction =  testData.map { point =>
      val prediction =  model.predict(Vectors.dense(point))
      prediction
    }
   prediction
  }

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
       val labeledLogs = dataPreparation.labeledPointRDDOfCPULogsMovingAverage(cpuLogs.filter(line => line.dateTime.
         isAfter( DateTime.now.minusYears(1) )),25)
//      val labeledLogs = DataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
//        isAfter(DateTime.now.minusYears(1))))

      prediction = GenerateCPUPredictions.extrapolateCPULogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Month prediction
     */
    if(extrapolationDuration.equals("monthly"))
    {
      val labeledLogs = dataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusMonths(1))))

      prediction = GenerateCPUPredictions.extrapolateCPULogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Week prediction
     */
    if(extrapolationDuration.equals("weekly"))
    {
      val labeledLogs = dataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(1))))

      prediction = GenerateCPUPredictions.extrapolateCPULogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Fortnight prediction
     */
    if(extrapolationDuration.equals("fortnightly"))
    {
      val labeledLogs = dataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusWeeks(2))))

      prediction = GenerateCPUPredictions.extrapolateCPULogs(labeledLogs, sc, extrapolationType)
    }

    /*
    Day prediction
     */
    if(extrapolationDuration.equals("daily"))
    {
      val labeledLogs = dataPreparation.labeledPointRDDOfCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter(DateTime.now.minusDays(1))))

      prediction = GenerateCPUPredictions.extrapolateCPULogs(labeledLogs, sc, extrapolationType)
    }

   prediction
  }

}
