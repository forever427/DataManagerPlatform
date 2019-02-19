package com.wch.app.dataLabelling

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object LocationTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]

    val provincename: String = row.getAs[String]("provincename")
    val cityname: String = row.getAs[String]("cityname")

    if (StringUtils.isNotBlank(provincename)) {
      list :+= (provincename, 1)
    }
    if (StringUtils.isNotBlank(cityname)) {
      list :+= (cityname, 1)
    }

    list
  }
}
