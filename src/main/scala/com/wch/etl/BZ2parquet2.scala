package com.wch.etl

import com.wch.beans.Log
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object BZ2parquet2 {
  def main(args: Array[String]): Unit = {
    // 判断参数的个数是否为2个
    if (args.length != 2) {
      println("输入输出路径异常, 程序退出!")
      sys.exit()
    }

    val Array(inputPath, outputPath) = args

    // 模板代码
    val conf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(s"${this.getClass.getSimpleName}")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 读取文件
    val lines: RDD[String] = ss.sparkContext.textFile(inputPath)
    // 清洗数据
    val rowRDD: RDD[Log] = lines.map(t=>t.split(",",t.length)).filter(_.length>=85).map(Log(_))

    // 创建DataFrame
    val df: DataFrame = ss.createDataFrame(rowRDD)
    // 写入数据
    df.write.partitionBy("provincename","cityname").parquet(outputPath)
    // 关闭资源
    ss.stop()
  }
}
