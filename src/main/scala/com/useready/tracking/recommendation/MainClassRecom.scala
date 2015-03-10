package com.useready.tracking.recommendation

import com.useready.tracking.CheckThreshold
import org.apache.spark.SparkContext
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// Import Spark SQL data types and Row.
import org.apache.spark.sql._
/**
 * Created by Ashu on 05-03-2015.
 */

/*
This is a class for header hash map
 */
class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
  def apply(ind:Int):String = header(ind)
}

object MainClassRecom {

  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "winutil\\")  //comment out for linux
    val sc = new SparkContext("local", "recom")
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // Create an RDD
    val ramPB = sc.textFile("data/ram_process.csv")

    val data = ramPB.map(line => line.split(","))
      .map(line => line
      .map(w => w.replaceAll("^\"|\"$", "")))           //lines in rows

    //get how many columns are there in the file
    val nColumn = data.take(1)(0).length
    val header = new SimpleCSVHeader(data.take(1)(0)) // we build our header with the first line


    // The schema is encoded in a string header
    val schemaString = data.take(1)(0).mkString(",")

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))

    // filter the header out
    val rows = data.filter(line => header(line,"worker") != "worker")
    val myRDD = rows.map(p=>Row.fromSeq(p.toSeq))

    // Apply the schema to the RDD.
    val privateByteSchemaRDD = sqlContext.applySchema(myRDD, schema)

    // Register the SchemaRDD as a table.
    privateByteSchemaRDD.registerTempTable("ramPB")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT * FROM ramPB")

    val durationList=Seq("mor","day","eve","nig")
    val RecommendationTime =  DateTime.now()
    val counter = "RAM"
    val worker = header(rows.first(),"worker")

    for (duration <- durationList){
      var data :SchemaRDD = null
      //now fetch morining day eve and night data
      duration match {
        case "mor"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=4 &&
          fm.parseDateTime( t(1).toString ).getHourOfDay()<10)

        case "day"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=10 &&
          fm.parseDateTime( t(1).toString ).getHourOfDay()<16)

        case "eve"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=16 &&
          fm.parseDateTime( t(1).toString ).getHourOfDay()<22)

        case "nig"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=22 &&
          fm.parseDateTime( t(1).toString ).getHourOfDay()<4)
      }
      if(data.count()>0)
        Recommendation.generateStats(data,nColumn,worker,duration,schemaString,header,counter,
          RecommendationTime,sqlContext,sc)
    }
    

  }

}
