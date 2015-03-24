package com.useready.tracking.recommendation

import com.useready.tracking.CheckThreshold
import machinebalancing.app.{CloudBalancingHelloWorld, WorkerGenerator}
import machinebalancing.domain.{CloudBalance, CloudComputer}
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.optaplanner.core.api.solver.{Solver, SolverFactory}

// Import Spark SQL data types and Row.
import org.apache.spark.sql._
/**
 * Created by Ashu on 05-03-2015.
 */

object MainClassRecom {

  val periodList = Seq("mor", "day", "eve", "nig")
  val path = "data/"
  val counterList = List("RAM","DISK")
  val solverFactory: SolverFactory = SolverFactory.createFromXmlResource("machinebalancing/solver/cloudBalancingSolverConfig.xml")
  val solver: Solver = solverFactory.buildSolver

  def main(args: Array[String]) {
  System.setProperty("hadoop.home.dir", "winutil\\") //comment out for linux
    val sc = new SparkContext("local", "recom")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val recommendationTime = DateTime.now()
    val workerList = sc.textFile(path + "workers").zipWithIndex()
    .map(DomainObjectGenerator.createWorker)
      .filter(w => w.name != "x").collect().toList

    workerList.foreach(w=>println(w.id+ " cpu =>"+ w.cpu
      + " ram =>"+w.ram+ " disk =>"+ w.disk))
    for (worker <- workerList) {
      for (counter <- counterList) {

        val data = sc.textFile(path + worker.name + "-" + counter + ".csv")
        val (results, header, nColumn, schemaString) = DataParser.
          Parsedata(data, recommendationTime, sc, sqlContext)

//        println(worker)

        for (period <- periodList) {
          //get data for a period
          val periodData: SchemaRDD = PeriodFilter.filterData(period, results)
          //genrate recommendation stat
          if (periodData.count() > 0)
//            Recommendation.generateStats(periodData, nColumn, worker, period, schemaString, header, counter,
//              recommendationTime, sqlContext, sc)

          //create planner domain objects
          DomainObjectGenerator.CloudBalanceGenerator(periodData, counter, header, worker, period,nColumn,
            schemaString, sqlContext,sc)

        }
      }
    }

    DomainObjectGenerator.computerList.foreach(w=>println(w.getLabel+ " cpu =>"+ w.getCpuPower
      + " ram =>"+w.getMemory+ " disk =>"+ w.getNetworkBandwidth))
    DomainObjectGenerator.processList.foreach(w=>println(w.getLabel+ " cpu =>"+ w.getRequiredCpuPower
      + " ram =>"+w.getRequiredMemory+ " disk =>"+ w.getRequiredNetworkBandwidth))

    val unsolvedCloudBalance: CloudBalance =  DomainObjectGenerator.createCloudBlance()
    // Load a problem with 400 computers and 1200 processes
    // CloudBalance unsolvedCloudBalance = new CloudBalancingGenerator().createCloudBalance(5, 12);
    // Solve the problem
    solver.solve(unsolvedCloudBalance)
    val solvedCloudBalance: CloudBalance = solver.getBestSolution.asInstanceOf[CloudBalance]

    CloudBalancingHelloWorld.toDisplayString(solvedCloudBalance)
  }
}
