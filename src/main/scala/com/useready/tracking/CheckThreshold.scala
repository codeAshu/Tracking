package com.useready.tracking

/**
 * Created by abhilasha on 09-02-2015.
 */
object CheckThreshold {

  final val CPUThreshold = 80
  final val RAMThreshold = 80
  final val DiskThreshold = 75

  def cpuThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {

    if(predictedValues.max > CPUThreshold){
      return true
    }
    else
      return false
  }

  def ramThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {

    if(predictedValues.max > RAMThreshold){
      return true
    }
    else
      return false
  }

  def diskThresholdCrossed(predictedValues: IndexedSeq[Double]) : Boolean = {

    if(predictedValues.max > DiskThreshold){
      return true
    }
    else
      return false
  }
}
