/**
 * Created by Ashu on 22-01-2015.
 */
package com.useready.tracking
import org.apache.spark.SparkContext
import org.apache.spark.mllib.optimization.SimpleUpdater
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LinearRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime
import org.apache.spark.mllib.linalg.{Vectors, Vector}

object extrapolation {


  def extrapolateCPULogs(cpuLgs: RDD[CPULog], window: Int, sc: SparkContext) = {

    //handle partitions by replicating boundries which span across partitions
    val logs = cpuLgs.mapPartitionsWithIndex((i, p) => {
      val overlap = p.take(window - 1).toArray
      val spill = overlap.iterator.map((i - 1, _))
      val keep = (overlap.iterator ++ p).map((i, _))
      if (i == 0) keep else keep ++ spill
    }).map(line=> line._2)

    //smoothed out rdd by moving average with window
    val used =  logs.map(line => line.used).mapPartitions(p => {
      val sorted = p.toSeq.sorted
      val olds = sorted.iterator
      val news = sorted.iterator
      var sum = news.take(window - 1).sum
      (olds zip news).map({ case (o, n) => {
        sum += n
        val v = sum
        sum -= o
        v/window
      }})
    })

    /*
   create LabeledPoint RDD for the extrapolation function
   Label is just index as of now.
    */
    val labeledLogs =  used
      .zipWithIndex()
      .map{ line =>
      val vec = Vectors.dense(line._2.toDouble)
      LabeledPoint(line._1, vec)
    }.cache()

    //count of cpu logs
    val nCpuLogs = logs.count()

    //create extrapolation independent varible list
    val testData  = (nCpuLogs to 2*nCpuLogs).map(x=> x.toDouble)

    //call the function to extrapolate
    val model = extrapolate(labeledLogs,sc)
    println("regression model: " +model)

    val prediction =  testData.map { point =>
      val prediction =  model.predict(Vectors.dense(point))
      prediction
    }

    prediction.foreach(println)

  }


  /**
   * This function takes a Labeled RDD and apply linear regression over it
   * @param parsedData Labled RDD of logs
   * @param sc : SparkContext
   * @return Linear regression model
   */
  def extrapolate(parsedData: RDD[LabeledPoint], sc:SparkContext): LinearRegressionModel = {

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

  def main(args: Array[String]) {

    //This is only added for windows 8.1, its a bug in Spark and this workaround is find here
    //http://apache-spark-user-list.1001560.n3.nabble.com/rdd-saveAsTextFile-problem-td176.html
    System.setProperty("hadoop.home.dir", "G:\\winutils\\")  //comment out for linux

    val sc = new SparkContext("local", "extrapolation")
    val window = 10

    val cpuLogs = sc.textFile("data/cpu.csv",2).map(PerfmonLogs.parseCPULogLine)
      .filter(line => line.worker!="x")
      .cache()

    //generate CPU logs by day, fortnight, month, year


    /*
TODO :So based on the extrapolation window, decide the value of window
to smooth the analysis
day:window :10
fortnight :50
month :100
year :500
 */

    val dayprediction = extrapolateCPULogs(cpuLogs.filter(line => line.dateTime.
      isAfter( DateTime.now.minusDays(1) )), 10,sc)
    val fortnightPrediction = extrapolateCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter( DateTime.now.minusDays(15) )),50,sc)
    val monthPrediction = extrapolateCPULogs(cpuLogs.filter(line => line.dateTime.
        isAfter( DateTime.now.minusMonths(1) )),100,sc)
    val yearprediction = extrapolateCPULogs(cpuLogs.filter(line => line.dateTime.
      isAfter( DateTime.now.minusYears(1) )),500,sc)

  }

}

