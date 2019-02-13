package com.wch.app.regionalDistribution

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object sparkCoreActinve {
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
    // 将DataFrame 类型转换为 RDD 类型
    val rowRDD: RDD[Row] = df.rdd
    // 将 省市作为key, 转换RDD
    val provinceAndCityAsKey: RDD[((String, String), Row)] = rowRDD.map(x => ((x.getString(24), x.getString(25)), x))
    // 按照省市进行分组
    val groupedRDD: RDD[((String, String), Iterable[Row])] = provinceAndCityAsKey.groupByKey()
    // 按照条件进行分析求解
    val res = groupedRDD.map(x => {
      // 发来的所有原始请求数
      val rawRequest = x._2.filter(it => it.getInt(8) == 1 && it.getInt(35) >= 1).size
      // 筛选满足有效条件的请求数量
      val validRequest = x._2.filter(it => it.getInt(8) == 1 && it.getInt(35) >= 2).size
      // 筛选满足广告请求条件的请求数量
      val adRequest = x._2.filter(it => it.getInt(8) == 1 && it.getInt(35) == 3).size
      // 参与竞价的次数
      val joinBidding = x._2.filter(it => it.getInt(30) == 1 && it.getInt(31) == 1 && it.getInt(39) == 1).size
      // 成功竞价的次数
      val succBidding = x._2.filter(it => it.getInt(30) == 1 && it.getInt(31) == 1 && it.getInt(39) == 1 && it.getString(42).equals("1") && it.getInt(2) != 0).size
      // 竞价成功率
      val biddingSuccRate: Double = succBidding.toDouble / joinBidding.toDouble
      // 展示数    针对广告主统计：广告在终端实际被展示的数量
      val showCount = x._2.filter(it => it.getInt(30) == 1 && it.getInt(8) == 2).size
      // 点击数    针对广告主统计：广告展示后被受众实际点击的数量
      val clickCount = x._2.filter(it => it.getInt(30) == 1 && it.getInt(8) == 3).size
      // 点击率
      val clickRate: Double = clickCount.toDouble / showCount.toDouble
      // DSP
      val DSP: Iterable[Row] = x._2.filter(it => it.getInt(30) == 1 && it.getString(31).equals("1") && it.getString(42).equals("1"))
      // DSP广告成本   相对于投放DSP广告的广告主来说满足广告成功展示每次成本WinPrice/1000
      val adCost: Double = DSP.foldLeft(0.0)(_ + _.getDouble(41))
      // DSP广告消费   相对于投放DSP广告的广告主来说满足广告成功展示每次消费adpayment/1000
      val adConsumer: Double = DSP.foldLeft(0.0)(_ + _.getDouble(75))
      // 将需要的值进行返回
      (x._1._1, x._1._2, rawRequest, validRequest, adRequest, joinBidding, succBidding, biddingSuccRate, showCount, clickCount, clickRate, adCost / 1000, adConsumer / 1000)
    })
    // res.foreach(println(_))
    // 存储到磁盘中
    res.saveAsTextFile(outputPath)
  }
}
