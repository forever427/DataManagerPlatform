package com.wch.app.regionalDistribution

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.wch.constant.Constants
import com.wch.utils.DBUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object sparkSQLActinve {
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
    val sql = "select provincename,cityname," +
      "count(case when requestmode=1 and processnode>=1 then 1 else null end) as rawrequest," +
      "count(case when requestmode=1 and processnode>=2 then 1 else null end) as validrequest," +
      "count(case when requestmode=1 and processnode=3 then 1 else null end) as adrequest," +
      "count(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else null end) as joinbidding," +
      "count(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else null end) as succbidding," +
      "count(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else null end)/count(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else null end) as biddingsuccrate," +
      "count(case when requestmode=2 and  iseffective=1 then 1 else null end) as showcount," +
      "count(case when requestmode=3 and  iseffective=1 then 1 else null end) as clickcount," +
      "count(case when requestmode=3 and  iseffective=1 then 1 else null end)/count(case when requestmode=2 and  iseffective=1 then 1 else null end) as clickrate," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0 end)/1000 as adcost," +
      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0 end)/1000 as adconsumer " +
      "from etled_data group by provincename,cityname"

    // 创建配置文件
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://localhost:3306/dmp?useUnicode=true&characterEncoding=utf8"
    // 写入mysql
    ss.sql(sql).write.mode("overwrite").jdbc(url,"area_distribution",prop)
  }
}
