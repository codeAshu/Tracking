package com.useready.tracking.recommendation

import com.useready.tracking.{DISK, CPU}
import com.useready.tracking.recommendation.Period.Period
import machinebalancing.domain.{CloudBalance, CloudProcess, CloudComputer}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.collection.JavaConverters._

object Period extends Enumeration {
  type Period = Value
  val mor,day,eve,nig = Value
}
case class Worker(id:Long, name :String, cpu :Int, ram: Int, disk:Int )

object DomainObjectGenerator   {
  var computerList: List[CloudComputer] = List()
  var processList: List[CloudProcess]  = List()

  def createWorker(line :(String,Long)) = {
    try {
      val sVec = line._1.split(",").toVector
      val id = line._2
      val vec = sVec.map(w => w.replaceAll("^\"|\"$", ""))
      val name = vec(0)
      val cpu = vec(1).toInt
      val ram = vec(2).toInt
      val disk = vec(3).toInt
      Worker(id, name, cpu, ram, disk)
    } catch {
      //filter these later worker "x" is erroneous log line
      case e: Exception =>
        Worker(0, "x", 10, 10, 1000)
    }
  }
  def genrateWorkerId(id: Long, period: Period) : Long = (id.toString + period.id).toLong
  def genrateprocessId(id: Long,period: Period, i: Int) : Long = (id.toString + period.id + i.toString).toLong
  def getComputerById(id: Long): CloudComputer = computerList.find(c => c.getId() ==  id).head
  def getProcessById(id: Long): CloudProcess = processList.find(c => c.getId() ==  id).head

  def isComputerExist(id : Long) :Boolean = {
    if (computerList == null) false
    else{
      if (computerList.exists(c => c.getId() == id)) true
      else  false
    }

  }
  def isProcessExist(id: Long)  :Boolean = {
    if (processList == null) false
    else{
      if (processList.exists(c => c.getId() == id)) true
      else  false
    }

  }

  def CloudBalanceGenerator(logs: SchemaRDD,
                            counter: String,
                            header: SimpleCSVHeader,
                            worker :Worker,
                            period :String,
                            nCol: Int,
                            schemaString: String,
                            sqlContext: SQLContext,
                            sc: SparkContext) = {

    computerGenerator(worker, period, counter)
    processgenerator(worker, logs,period,counter,nCol,sqlContext,schemaString, sc)

  }
  def createCloudBlance() :CloudBalance = {
    val c: CloudBalance = new CloudBalance
    c.setComputerList(computerList.asJava)
    c.setProcessList(processList.asJava)
    c.setId(0l)
    c
  }

  def computerGenerator(worker: Worker, period: String, counter: String) = {
    //fetch the numerical id of worker assign it as worker id and period id
    val id = genrateWorkerId(worker.id,Period.withName(period))
    var computer: CloudComputer = null
    if(!isComputerExist(id)) {
      computer = new CloudComputer
      computer.setId(id)
    }
    else  computer = getComputerById(id)
    counter match  {
      case "CPU" =>  computer.setCpuPower(worker.cpu)
      case "RAM" => computer.setMemory(worker.ram)
      case "DISK" =>  computer.setNetworkBandwidth(worker.disk)
    }
    if(!isComputerExist(id)) computerList ::= computer
  }


  def processgenerator(worker: Worker,
                      logs: SchemaRDD,
                      period: String,
                      counter: String,
                      nCol :Int,
                      sqlContext :SQLContext,
                      schemaString: String,
                      sc :SparkContext): Unit = {

    //create a vector of each process
    val logVec = logs.map(t => t.toVector.map(e => e.toString))
    var i = 0
    for (i <- 3 to nCol-1) {
      val p = logVec.map(w => w(i).toDouble)
        .collect()

      val mean = (p.sum / p.size)
      val id = genrateprocessId(worker.id, Period.withName(period), i)
      var process: CloudProcess = null
      if (!isProcessExist(id)) {
        process = new CloudProcess
        process.setId(id)
      }
      else process = getProcessById(id)
      counter match {
        //convert to gigahertz speed
        case "CPU" => process.setRequiredCpuPower( (mean*0.01*worker.cpu).toInt)
        //convert into MB
        case "RAM" => process.setRequiredMemory((mean/1000000.).toInt)
        //convert into GB
        case "DISK" => process.setRequiredNetworkBandwidth((mean/10*9.toDouble).toInt)
      }
      if (!isProcessExist(id)) processList ::= process
    }
  }



}
