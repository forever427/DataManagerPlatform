package com.wch.app.provincesAndCitiesDataQuantityDistribution

import java.io.PrintWriter
import java.sql.{Connection, PreparedStatement}

import com.alibaba.fastjson.JSONObject
import com.wch.utils.DBUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object sparkCoreAchieve {

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

    // 将DataFrame 类型转换为rdd类型
    val rowRDD: RDD[Row] = df.rdd
    // 将省市作为key, 转换rdd
    val provinceAndCityAsKey: RDD[((String, String), String)] = rowRDD.map(x => ((x.getString(24), x.getString(25)), x.toString()))

    // 按照省市分组进行累加求和得到map集合 Map((省,市),个数)
    val provinceAndCityCount: collection.Map[(String, String), Long] = provinceAndCityAsKey.countByKey()
    // {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
    // 将map集合中的每一个元素变成一个json 串
    val countJson: Iterable[JSONObject] = provinceAndCityCount.map(x => {
      val jSONObject = new JSONObject()
      jSONObject.put("ct", x._2)
      jSONObject.put("provincename", x._1._1)
      jSONObject.put("cityname", x._1._2)
      jSONObject
    })

    // 使用 JDBC 工具类获取连接
    val conn: Connection = DBUtils.getConnection()
    // 创建SQL插入语句
    val sql = "insert into provinces_cities_data_distribution values(?,?,?)"
    // 创建ps对象
    val ps: PreparedStatement = conn.prepareStatement(sql)
    // 给SQL语句中的 ? 赋值, 并执行SQL语句
    provinceAndCityCount.foreach(x => {
      ps.setLong(1, x._2)
      ps.setString(2, x._1._1)
      ps.setString(3, x._1._2)
      ps.executeUpdate()
    })
    // 关闭资源
    conn.close()
    // 创建写出流
    val pw = new PrintWriter(outputPath)
    // 遍历每一个元素, 将json串写入文件中
    countJson.foreach(x => {
      pw.write(x.toJSONString)
      pw.write("\n")
    })
    // 关闭资源
    pw.close()
    ss.stop()
  }
}


