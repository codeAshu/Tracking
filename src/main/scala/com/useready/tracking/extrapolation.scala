/**
 * Created by Ashu on 22-01-2015.
 */
package com.useready.tracking

import org.apache.spark.SparkContext
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.regression._
import org.apache.spark.rdd.RDD

object extrapolation {

  /**
   * This function takes a Labeled RDD and apply linear regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Linear regression model
   */
  def extrapolateLinear(parsedData: RDD[LabeledPoint], sc:SparkContext): LinearRegressionModel = {

    val algorithm = new LinearRegressionWithSGD()
    algorithm.optimizer.setStepSize(0.0001)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new SimpleUpdater())
    algorithm.optimizer.setRegParam(0.01)

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
    sc.stop()
    return model
  }

  /**
   * This function takes a Labeled RDD and apply Ridge regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Ridge regression model
   */
  def extrapolateRidge(parsedData: RDD[LabeledPoint], sc:SparkContext): RidgeRegressionModel = {

    val algorithm = new RidgeRegressionWithSGD()
    algorithm.optimizer.setStepSize(0.0001)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new SimpleUpdater())
    algorithm.optimizer.setRegParam(0.01)

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
    sc.stop()
    return model
  }


  /**
   * This function takes a Labeled RDD and apply Lasso regression over it
   * @param parsedData Labeled RDD of logs
   * @param sc : SparkContext
   * @return Lasso regression model
   */
  def extrapolateLasso(parsedData: RDD[LabeledPoint], sc:SparkContext): LassoModel = {

    val algorithm = new LassoWithSGD()
    algorithm.optimizer.setStepSize(0.0001)
    algorithm.optimizer.setNumIterations(100)
    algorithm.optimizer.setUpdater(new SimpleUpdater())
    algorithm.optimizer.setRegParam(0.01)

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
    sc.stop()
    return model
  }

}

