package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.optimization.L1Updater
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD


object extrapolation {
  /**
   *
   * @param logs
   * @param sc
   * @param extrapolationType
   * @return
   */

  def extrapolateLogs(counter: String,
                      logs: RDD[LabeledPoint],
                      sc: SparkContext,
                      extrapolationType: String) : IndexedSeq[(Double,Double)] = {

    //count of logs
    val nLogs = logs.count()

    //create extrapolation independent variable list
    val testData = (nLogs to 2 * nLogs).map(x => x.toDouble)

    //call the function to extrapolate
    var model: GeneralizedLinearModel = null

    if(extrapolationType.equals("ridge"))
      model = extrapolation.extrapolateRidge(counter,logs, sc)

    else if(extrapolationType.equals("lasso"))
      model = extrapolation.extrapolateLasso(counter,logs, sc)

    else model = extrapolation.extrapolateLinear(counter,logs, sc)

    println("regression model: " +model)

    val prediction =  testData.map { point =>
      val prediction =  model.predict(Vectors.dense(point))
      (point, prediction)
    }

    prediction
  }

  /**
   * This function takes a Labeled RDD and apply linear regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Linear regression model
   */
  def extrapolateLinear(counter: String ,
                        parsedData: RDD[LabeledPoint],
                        sc:SparkContext): LinearRegressionModel = {
    var stepSize = 0.001
    counter match {
      case "CPU" => stepSize = CPU.extrpolateStepSize
      case "DISK" => stepSize = DISK.extrpolateStepSize
      case "RAM" => stepSize = RAM.extrpolateStepSize
    }
    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer.setStepSize(stepSize)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new SimpleUpdater())

    val model = algorithm.run(parsedData)

      val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (prediction,point.label)
    }

    val loss = valuesAndPreds.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / parsedData.count())

    println(s"RMSE = $rmse.")
    model
  }

  /**
   * This function takes a Labeled RDD and apply Ridge regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Ridge regression model
   */
  def extrapolateRidge(counter: String,
                       parsedData: RDD[LabeledPoint],
                       sc:SparkContext): RidgeRegressionModel = {

    var stepSize = 0.001
    counter match {
      case "CPU" => stepSize = CPU.extrpolateStepSize
      case "DISK" => stepSize = DISK.extrpolateStepSize
      case "RAM" => stepSize = RAM.extrpolateStepSize
    }

    val algorithm = new RidgeRegressionWithSGD()
    algorithm.optimizer.setStepSize(stepSize)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new SquaredL2Updater())
    algorithm.optimizer.setRegParam(1.0)

    val model = algorithm.run(parsedData)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (prediction,point.label)
    }

    val loss = valuesAndPreds.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / parsedData.count())

    println(s"RMSE = $rmse.")
    model
  }


  /**
   * This function takes a Labeled RDD and apply Lasso regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Lasso regression model
   */
  def extrapolateLasso(counter: String,
                       parsedData: RDD[LabeledPoint],
                       sc:SparkContext): LassoModel = {

    var stepSize = 0.001
    counter match {
      case "CPU" => stepSize = CPU.extrpolateStepSize
      case "DISK" => stepSize = DISK.extrpolateStepSize
      case "RAM" => stepSize = RAM.extrpolateStepSize
    }

    val algorithm = new LassoWithSGD()
    algorithm.optimizer.setStepSize(stepSize)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new L1Updater())
    algorithm.optimizer.setRegParam(0.1)

    val model = algorithm.run(parsedData)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (prediction,point.label)
    }

    val loss = valuesAndPreds.map { case (p, l) =>
      val err = p - l
      err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / parsedData.count())

    println(s"RMSE = $rmse.")
    model
  }

}

