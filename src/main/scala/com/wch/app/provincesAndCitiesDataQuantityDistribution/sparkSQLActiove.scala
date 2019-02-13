package com.wch.app.provincesAndCitiesDataQuantityDistribution

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object sparkSQLActiove {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入输出路径异常!")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = ss.read.parquet(inputPath)

    df.createOrReplaceTempView("etled_data")

    val sql = "select count(1) ct,provincename,cityname from etled_data group by provincename,cityname"

    val res: DataFrame = ss.sql(sql)

    // 写入文件
    res.coalesce(1).write.mode("overwrite").json(outputPath)

    // 创建配置文件
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://localhost:3306/dmp?useUnicode=true&characterEncoding=utf8"
    // 写入mysql
    res.write.mode("overwrite").jdbc(url,"provinces_cities_data_distribution",prop)

  }
}
