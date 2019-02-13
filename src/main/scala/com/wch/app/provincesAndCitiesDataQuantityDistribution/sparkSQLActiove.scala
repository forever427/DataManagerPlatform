package com.wch.app.provincesAndCitiesDataQuantityDistribution

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object sparkSQLActiove {
  def main(args: Array[String]): Unit = {
    // 判断输入输出路径
    if (args.length != 2) {
      println("输入输出路径异常!")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args

    // 模板代码
    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 读取 parquet 格式文件
    val df: DataFrame = ss.read.parquet(inputPath)
    // 创建临时表
    df.createOrReplaceTempView("etled_data")
    // 创建SQL语句
    val sql = "select count(1) ct,provincename,cityname from etled_data group by provincename,cityname"
    // 使用sparkSQL查询
    val res: DataFrame = ss.sql(sql)

    // 将查询结果写入文件
    res.coalesce(1).write.mode("overwrite").json(outputPath)

    // 创建配置文件
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://localhost:3306/dmp?useUnicode=true&characterEncoding=utf8"
    // 写入mysql
    res.write.mode("overwrite").jdbc(url, "provinces_cities_data_distribution", prop)

  }
}
