package com.wch.app.dataLabelling

import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object BusinessTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    val row: Row = args(0).asInstanceOf[Row]
    val jedis: Jedis = args(1).asInstanceOf[Jedis]
    var list = List[(String, Int)]()
    if (row.getAs[String]("long").toDouble >= 73.66 && row.getAs[String]("long").toDouble <= 135.05 &&
      row.getAs[String]("lat").toDouble >= 3.86 && row.getAs[String]("lat").toDouble <= 53.55) {
      val lat: String = row.getAs[String]("lat")
      val long: String = row.getAs[String]("long")
      val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
      val business: String = jedis.get(geoHash)
      business.split(";").foreach(t => list :+= (t, 1))
    }
    list
  }
}
