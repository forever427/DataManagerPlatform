package com.wch.app.dataLabelling

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeywordsTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    val row: Row = args(0).asInstanceOf[Row]
    val stopwords: Broadcast[Map[String, Int]] = args(1).asInstanceOf[Broadcast[Map[String, Int]]]

    // 关键字
    val keywords: String = row.getAs("keywords")
    val kws: Array[String] = keywords.split("\\|").filter(k => k.length >= 3 && k.length <= 8 && !stopwords.value.contains(k)).map("K" + _)
    // ks.foreach(println)
    kws.foreach(list :+= (_, 1))

    list
  }
}
