package com.wch.app.dataLabelling

import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val dict: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String,String]]]

    // APP名称
    val appname: String = row.getAs("appname")
    val appid: String = row.getAs[String]("appid")
    if (StringUtils.isNotBlank(appname)) {
      list:+=("APP"+appname,1)
    }else if (StringUtils.isNotBlank(appid)){
      list:+=(dict.value.getOrElse(appid,appid),1)
    }

    list
  }
}
