
import java.io.{IOException, FileNotFoundException}

import com.useready.tracking.utils.PerfmonLogWriter
import com.useready.tracking._
import _root_.utils.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.eclipse.jetty.plus.jaas.spi.PropertyFileLoginModule
import org.joda.time.DateTime
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class perfmonLogParserSuit extends FunSuite with BeforeAndAfterEach {

  var logLine: String = _

  test("wrong cpu log line should parse as default worker 'x'") {
    logLine = "02/18/2015 19:06:32.595,10.50727028"
    val cpuLog = PerfmonLogs.parseCPULogLine(logLine)
    assert(cpuLog.worker == "x")
  }

  test("wrong ram log line should parse as default worker 'x'") {
    logLine = "02/18/2015 19:06:32.595,10.50727028"
    val ramLog = PerfmonLogs.parseRAMLogLine(logLine)
    assert(ramLog.worker == "x")
  }

  test("wrong disk log line should parse as default worker 'x'") {
    logLine = "02/18/2015 19:06:32.595,10.50727028"
    val diskLog = PerfmonLogs.parseDiskLogLine(logLine)
    assert(diskLog.worker == "x")
  }

  test("file should be comma separated (csv)") {
    logLine = "w1   02/18/2015 19:06:32.595   10.50727028"
    val cpuLog = PerfmonLogs.parseCPULogLine(logLine)
    assert(cpuLog.worker == "x")
  }

}

  class GenerateAllPredictionsSuit extends FunSuite with BeforeAndAfterEach with LocalSparkContext {
    var filePath: String = _
    var worker = "w1"
    var interval = 2
    var algo = "linear"
    var time = DateTime.now()
    var name = "CPU"
    var parser = PerfmonLogs.parseCPULogLine(_)
    var fileCreater = PerfmonLogWriter.createCPUFile(_)
    var logWriter = PerfmonLogWriter.cpuLogWriter(_, _, _, _, _)

    test("should handel IO and File not found exception for CPU") {
      filePath = "data/xyz.csv"
      val time = DateTime.now()
      intercept[TestFailedException] {
        intercept[IOException] {
          GenerateAllPredictions.generatePredictionHof(sc, worker, interval, algo, time, name, filePath,
            parser, fileCreater, logWriter)
        }
      }
    }
  }
class DatapreprationSuit extends FunSuite with BeforeAndAfterEach with LocalSparkContext {
  var worker = "w1"
  var interval = 2
  var time = DateTime.now()
  var window = 3


    test("when window is less then data do the moving average") {
    val logRdd = sc.parallelize(Seq(
    Log(worker, time, 100.0, 10.0, 90.0, "R"),
    Log(worker, time, 100.0, 20.0, 80.0, "R"),
    Log(worker, time, 100.0, 30.0, 70.0, "R"),
    Log(worker, time, 100.0, 40.0, 60.0, "R")))

        val prepData = DataPreparation.labeledPointRDDMovingAverage(logRdd, window)
        assert(prepData.count() == 2)
    }

  test("moving average function should work correctly") {
    val logRdd = sc.parallelize(Seq(
    Log(worker, time, 100.0, 10.0, 90.0, "R"),
    Log(worker, time, 100.0, 20.0, 80.0, "R"),
    Log(worker, time, 100.0, 30.0, 70.0, "R"),
    Log(worker, time, 100.0, 40.0, 60.0, "R")))

    //since window is 3 so first used data will be (10+20+30)/3 = 20
    val prepData = DataPreparation.labeledPointRDDMovingAverage(logRdd, window)
    val first = prepData.first().label
    assert(first == 20.0)
    //second used should be (20+30+40)/3 = 30
    val second = prepData.take(2)(1).label
    assert(second == 30.0)
  }

  test("if window is more then data then no moving avg should not be performed") {
    window = 10
    val logRdd = sc.parallelize(Seq(
      Log(worker, time, 100.0, 10.0, 90.0, "R"),
      Log(worker, time, 100.0, 20.0, 80.0, "R"),
      Log(worker, time, 100.0, 30.0, 70.0, "R"),
      Log(worker, time, 100.0, 40.0, 60.0, "R")))

    val prepData = DataPreparation.labeledPointRDDMovingAverage(logRdd, window)
    assert(prepData.count() == logRdd.count())
  }
}
class extrapolationSuit extends FunSuite with BeforeAndAfterEach with LocalSparkContext {

  //TODO: write function and validation tests for algorithms
}


