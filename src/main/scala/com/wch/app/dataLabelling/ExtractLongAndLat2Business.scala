package com.wch.app.dataLabelling

import ch.hsr.geohash.GeoHash
import com.wch.utils.{BaiduLBSUtils, JedisUtils, SparkUtils}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import redis.clients.jedis.Jedis

object ExtractLongAndLat2Business {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("输入路径错误,程序退出!")
      sys.exit()
    }
    val Array(inputPath) = args
    val ss: SparkSession = SparkUtils.getSession

    val longAndLat: Dataset[Row] = ss.read.parquet(inputPath).select("long", "lat").filter("cast(long as double) >=73.66 and cast(long as double) <=135.05 and cast(lat as double) >=3.86 and cast(lat as double) <= 53.55").distinct()

    longAndLat.foreachPartition(part => {
      val jedis: Jedis = JedisUtils.getConnection()
      part.foreach(t => {
        val long: String = t.getAs[String]("long")
        val lat: String = t.getAs[String]("lat")

        val geohash: String = GeoHash.geoHashStringWithCharacterPrecision(lat.toDouble, long.toDouble, 8)
        val business: String = BaiduLBSUtils.parseBusinessTagBy(long, lat)
        if (StringUtils.isNotBlank(business)) {
          jedis.set(geohash, business)
        }
      })
      jedis.close()
    })

  }
}
