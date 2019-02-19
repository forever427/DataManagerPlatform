package com.wch.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSession = {
    // 模板代码
    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    ss
  }

}
