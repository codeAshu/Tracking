package com.useready.tracking.recommendation

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Created by Ashu on 12-03-2015.
 */
/*
This is a class for header hash map
 */
class SimpleCSVHeader(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def apply(array:Array[String], key:String):String = array(index(key))
  def apply(ind:Int):String = header(ind)
}

object DataParser {
  val fm = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss.SSS")

  def Parsedata(logs: RDD[String],
                recommendationTime: DateTime,
                sc: SparkContext,
                sqlContext: SQLContext) = {

    val data = logs.map(line => line.split(","))
      .map(line => line
      .map(w => w.replaceAll("^\"|\"$", ""))) //lines in rows

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
    val rows = data.filter(line => header(line, "worker") != "worker")
    val myRDD = rows.map(p => Row.fromSeq(p.toSeq))

    // Apply the schema to the RDD.
    val privateByteSchemaRDD = sqlContext.applySchema(myRDD, schema)

    // Register the SchemaRDD as a table.
    privateByteSchemaRDD.registerTempTable("ramPB")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT * FROM ramPB")

    (results,header,nColumn,schemaString)
  }






}
