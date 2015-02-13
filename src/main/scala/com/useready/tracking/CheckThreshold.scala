package com.useready.tracking

/**
 * Created by abhilasha on 09-02-2015.
 */
object CheckThreshold {

  final val CPUThreshold = 80
  final val RAMThreshold = 80
  final val DiskThreshold = 75

  def cpuThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {
    predictedValues.max > CPUThreshold
  }

  def ramThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {
    predictedValues.max > RAMThreshold
  }

  def diskThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {
    predictedValues.max > DiskThreshold
  }
}
