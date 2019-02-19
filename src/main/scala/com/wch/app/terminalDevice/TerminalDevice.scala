package com.wch.app.terminalDevice

import java.util.Properties

import com.wch.conf.ConfigurationManager
import com.wch.constant.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object TerminalDevice {
  def main(args: Array[String]): Unit = {
    // 判断输入输出路径
    if (args.length != 1) {
      println("输入路径异常!")
      sys.exit()
    }
    val inputPath = args(0)

    // 模板代码
    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 读取 parquet 格式文件
    val df: DataFrame = ss.read.parquet(inputPath)
    // 创建临时表
    df.createOrReplaceTempView("etled_data")
    // 创建运营商类型SQL语句
    val ispnameSql = "select ispname," +
      "count(case when requestmode=1 then 1 else null end) as rawrequest," +
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
      "from etled_data group by ispname"

    // 创建操作系统类型SQL语句
    val clientSql = "select case client when 1 then 'android' when 2 then 'ios' when 3 then 'wp' when 4 then 'wp' else '其他'end as client," +
      "count(case when requestmode=1 then 1 else null end) as rawrequest," +
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
      "from etled_data group by client"

    // 创建网络类型SQL语句
    val networkmannernameSql = "select networkmannername," +
      "count(case when requestmode=1 then 1 else null end) as rawrequest," +
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
      "from etled_data group by networkmannername"

    // 创建设备类型SQL语句
    val devicetypeSql = "select case devicetype when 1 then '手机' when 2 then '平板' else '其他'end as devicetype," +
      "count(case when requestmode=1 then 1 else null end) as rawrequest," +
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
      "from etled_data group by devicetype"
    // 创建配置文件
    val prop = new Properties()
    prop.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.put("password",ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    // 写入mysql
    ss.sql(ispnameSql).write.mode("overwrite").jdbc(url,"ispname",prop)
    ss.sql(networkmannernameSql).write.mode("overwrite").jdbc(url,"networkmannername",prop)
    ss.sql(clientSql).write.mode("overwrite").jdbc(url,"client",prop)
    ss.sql(devicetypeSql).write.mode("overwrite").jdbc(url,"devicetype",prop)
    ss.stop()
  }
}
