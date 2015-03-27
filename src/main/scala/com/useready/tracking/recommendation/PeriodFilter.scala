package com.useready.tracking.recommendation

import org.apache.spark.sql.SchemaRDD

/**
 * Created by Ashu on 20-03-2015.
 */
object PeriodFilter {
  val fm = DataParser.fm
  def filterData(period: String, results: SchemaRDD): SchemaRDD ={
    var data :SchemaRDD = null

    //now fetch morning day eve and night data
    period match {
      case "mor"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=4 &&
        fm.parseDateTime( t(1).toString ).getHourOfDay()<10)

      case "day"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=10 &&
        fm.parseDateTime( t(1).toString ).getHourOfDay()<16)

      case "eve"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=16 &&
        fm.parseDateTime( t(1).toString ).getHourOfDay()<22)

      case "nig"=> data = results.filter(t=> fm.parseDateTime( t(1).toString ).getHourOfDay()>=22
        && fm.parseDateTime( t(1).toString ).getHourOfDay()<24 ||
        fm.parseDateTime( t(1).toString ).getHourOfDay()>=0 &&
          fm.parseDateTime( t(1).toString ).getHourOfDay()<4)
    }
  data
  }

}
