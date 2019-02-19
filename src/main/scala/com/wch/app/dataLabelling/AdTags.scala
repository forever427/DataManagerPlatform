package com.wch.app.dataLabelling

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object AdTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    // 广告位类型
    val adspacetype: Int = row.getAs[Int]("adspacetype")
    if (adspacetype < 10) list:+=("LC0" + adspacetype.toString, 1)
    else list:+=("LC" + adspacetype.toString, 1)

    // 广告位名称
    val adspacetypename: String = row.getAs[String]("adspacetypename")
    if(StringUtils.isNotBlank(adspacetypename))
    list:+=("LN" + adspacetypename,1)

    // 渠道
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")
    list:+=("CN" + adplatformproviderid,1)

    list
  }
}
