package com.wch.app.provincesAndCitiesDataQuantityDistribution

import java.io.PrintWriter

import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object sparkCoreActive {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("输入输出路径异常, 程序退出!")
      sys.exit()
    }
    val Array(input, output) = args

    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import ss.implicits._

    val df: DataFrame = ss.read.parquet(input)

    val rowRDD: RDD[Row] = df.rdd
    val provinceAndCityAsKey: RDD[((String, String), String)] = rowRDD.map(x => ((x.getString(24), x.getString(25)), x.toString()))

    val provinceAndCityCount: collection.Map[(String, String), Long] = provinceAndCityAsKey.countByKey()
    // {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
    val countJson: Iterable[JSONObject] = provinceAndCityCount.map(x => {
      val jSONObject = new JSONObject()
      jSONObject.put("ct", x._2)
      jSONObject.put("provincename", x._1._1)
      jSONObject.put("cityname", x._1._2)
      jSONObject
    })
    val pw = new PrintWriter(output)
    countJson.foreach(x=>{
      pw.write(x.toJSONString)
      pw.write("\n")
    })
    pw.close()



  }
}
