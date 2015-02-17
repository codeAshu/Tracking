package com.useready.tracking

/**
 * Created by abhilasha on 09-02-2015.
 */
object CheckThreshold {

  final val CPUThreshold = 80 //percentage
  final val RAMThreshold = 7000000000.00 //bytes
  final val DiskThreshold = 450000000000.00 //bytes

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
