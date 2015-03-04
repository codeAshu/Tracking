package com.useready.tracking

/**
 * Created by abhilasha on 09-02-2015.
 */
object CheckThreshold {
  /**
   *
   * @param predictData
   * @param counter
   * @return
   */
  def thresholdCrossed(predictData: IndexedSeq[(Double, Double)], counter: String): Boolean = {

    val  predictedValues = predictData.map(w=>w._2)

    counter match {
      case "CPU" =>  predictedValues.max > CPU.threshold
      case "DISK" => predictedValues.max > (DISK.threshold * DISK.total) / 100
      case "RAM" => predictedValues.max > (RAM.threshold * RAM.total) / 100
      case _ =>false
    }

  }
}

