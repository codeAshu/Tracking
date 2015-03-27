package com.useready.tracking.recommendation

import com.useready.tracking.CheckThreshold

import machinebalancing.app.{CloudBalancingHelloWorld, WorkerGenerator}
import machinebalancing.domain.{CloudProcess, CloudBalance, CloudComputer}
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.optaplanner.core.api.solver.{Solver, SolverFactory}
import org.apache.spark.sql._

/**
 * Created by Ashu on 05-03-2015.
 */
case class WorkerProcess(worker :Worker, header: SimpleCSVHeader) {}

object MainClassRecom {

  val periodList = Seq("mor", "day", "eve", "nig")
  val path = "data/"
  val counterList = List("RAM","DISK")
  val solverFactory: SolverFactory = SolverFactory.createFromXmlResource("machinebalancing/solver/cloudBalancingSolverConfig.xml")
  val solver: Solver = solverFactory.buildSolver
  var workerProcessList :List[WorkerProcess] = List()


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

        //append to worker process List
        val wl =  WorkerProcess(worker,header)
        workerProcessList ::=wl

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

    //create the problem
    val unsolvedCloudBalance: CloudBalance =  DomainObjectGenerator.createCloudBlance()

    // Solve the problem
    solver.solve(unsolvedCloudBalance)
    val solvedCloudBalance: CloudBalance = solver.getBestSolution.asInstanceOf[CloudBalance]

    //write the output
    RecomOutputWriter.writeRecommendation(solvedCloudBalance)

    //println(CloudBalancingHelloWorld.toDisplayString(solvedCloudBalance))

  }

}
