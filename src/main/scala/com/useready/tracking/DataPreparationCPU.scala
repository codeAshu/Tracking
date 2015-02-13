package com.useready.tracking

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * Created by abhilasha on 10-02-2015.
 */
object DataPreparationCPU {

  /**
  This method takes the RDD of CPU logs, does a moving average on the logs and returns a
  new RDD of labeled points on which extrapolation can be done
  */
  def labeledPointRDDOfCPULogsMovingAverage(cpuLgs: RDD[CPULog], window: Int) : RDD[LabeledPoint] = {

    //handle partitions by replicating boundaries which span across partitions
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
      (olds zip news).map({ case (o, n) =>
        sum += n
        val v = sum
        sum -= o
        v/window
      })
    })

//   create LabeledPoint RDD for the extrapolation function Label is just index as of now.
    val labeledLogs =  used
      .zipWithIndex()
      .map{ line =>
      val vec = Vectors.dense(line._2.toDouble)
      LabeledPoint(line._1, vec)
    }.cache()
    labeledLogs
  }

  /**
  This method takes an RDD of CPU logs and creates a labeled point RDD
  that is needed for extrapolation
   */
  def labeledPointRDDOfCPULogs(cpuLgs: RDD[CPULog]): RDD[LabeledPoint] ={

    val labeledLogs =  cpuLgs.map(line => line.used)
      .zipWithIndex()
      .map{ line =>
      val vec = Vectors.dense(line._2.toDouble)
      LabeledPoint(line._1, vec)
    }
    labeledLogs
  }
}
