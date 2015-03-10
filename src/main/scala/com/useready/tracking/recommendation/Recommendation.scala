package com.useready.tracking.recommendation


import com.useready.tracking.RAM
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.rddToPairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.joda.time.DateTime

/**
 * Created by Ashu on 09-03-2015.
 */
object Recommendation {

  //if RAM usages goes beyond 60% of its capacity, return true
  def crossedCheck(dPoint:Double, total:Double) : Boolean = dPoint > 0.60 * total

  /**
   * This function is generaic and generate percentage usage of logs
   * Each log should be of type <worker, DateTime, Total, process(1),process(2) ..... process(N)>
   * @param logs
   * @param nCol
   * @param worker
   * @param duration
   * @param schemaString
   * @param header
   * @param counter
   * @param RecommendationTime
   * @param sqlContext
   * @param sc
   */

  def generateStats(logs: SchemaRDD,
                     nCol: Int,
                     worker : String,
                     duration: String,
                     schemaString: String,
                     header: SimpleCSVHeader,
                     counter: String,
                     RecommendationTime: DateTime,
                     sqlContext: SQLContext,
                     sc: SparkContext) ={



    //get the total RAM
    val total = logs.map(t=>t(2)).first().toString.toDouble

    //create a table from logs RDD, ignore first three columns <worker,datetime,total>
    val rows = logs.map(t=>t.toArray.slice(3,nCol))

    val schema = StructType(
      schemaString.split(",").slice(3,nCol).map(fieldName => StructField(fieldName, StringType, true)))
    val myRDD = rows.map(p=>Row.fromSeq(p.toSeq))

    // Apply the schema to the RDD.
    val privateByteSchemaRDD = sqlContext.applySchema(myRDD, schema)

    // Register the SchemaRDD as a table.
    privateByteSchemaRDD.registerTempTable("ram")

    //select all processes
    val processRAM = sqlContext.sql("SELECT * FROM ram")

    //create a vector of each process
    val processVec = processRAM.map(t=>t.toVector.map(e=>e.toString))

    //create with threshold crossed information
    val threshVec = processVec.map(t=> t.map(x=>x.toDouble).foldLeft(0.)(_+_) )
    .map( w=> if(crossedCheck(w ,total)) (w,1) else (w,0) )

    // a normalized vector with first element as status(threshhold),Vector(sum, <all other process>)
    //here sum is addition of each process, used for normalization
    val percentVec = processVec.map{t=>
      val sum = t.map(x=>x.toDouble).foldLeft(0.)(_+_)
      val temp = t.map(e=>e.toDouble/sum)
      val w = Vector(sum)++temp
      if(crossedCheck( w(0),sum ))
        (1,w) else (0,w) }

    //print threshhold vectors
    threshVec.collect().foreach(println)
    val dataCount = threshVec.count()

    //check how much threshold is crossed
    val percentCrossed = (threshVec.map(w=>w._2).fold(0)(_+_).toDouble /dataCount)*100

    println(percentCrossed)

    //if 60% percent of time RAM/Disk/CPU goes beyond 60% of its capacity in a month
    //generate statistics, such as how much is the contribution of each process

    if(percentCrossed >60){

     //now identify which process has how much contribution
     val totalContr = percentVec.map(w=>w._2.tail)    //select all processes only
      .map(line=>line.zipWithIndex)                   //give each column an index
      .flatMap(w=>w.toSeq)
      .map(w => (w._2,w._1))
      .reduceByKey(_+_)
       .map(w=> (w._1,w._2/dataCount))

      //write contribution to file
      StatsWriter.counterStatsWriter(totalContr,worker,duration,header,counter, RecommendationTime,sc)

      //print
//      totalContr.map(w=> (w._1,w._2/dataCount))
//        .collect().foreach(println)
    }

  }

}
