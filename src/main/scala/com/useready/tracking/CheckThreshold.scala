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

    val CPUThreshold = 80 //percentage
    val RAMThreshold = 7000000000.00 //bytes
    val DiskThreshold = 450000000000.00 //bytes

    counter match {
      case "CPU" =>  predictedValues.max > CPUThreshold
      case "DISK" => predictedValues.max > DiskThreshold
      case "RAM" => predictedValues.max > RAMThreshold

    }

  }
}

