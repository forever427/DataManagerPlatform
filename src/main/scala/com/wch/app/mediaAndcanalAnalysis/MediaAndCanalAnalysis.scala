package com.wch.app.mediaAndcanalAnalysis

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

import com.wch.conf.ConfigurationManager
import com.wch.constant.Constants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object MediaAndCanalAnalysis {
  def main(args: Array[String]): Unit = {
    var inputPath: String = null
    // 判断输入输出路径
    if (args.length == 0) {
      println("请输入路径")
      val br = new BufferedReader(new InputStreamReader(System.in))
      inputPath = br.readLine()
    }
    else if (args.length == 1) {
      inputPath = args(0)
    }
    else {
      println("输入路径异常!")
      sys.exit()
    }

    // 模板代码
    val conf: SparkConf = new SparkConf().setAppName(s"${this.getClass.getSimpleName}").setMaster("local[2]").set("spark.debug.maxToStringFields","100")
    val ss: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 将字典文件进行读取, 抽取出需要的字段, 并转换成DataFrame
    val appDict: RDD[String] = ss.sparkContext.textFile("E:\\BigData\\06-Spark\\project\\dmp\\DataManagerPlatform\\src\\main\\scala\\com\\wch\\data\\app_dict.txt")
    val appDictRDD: RDD[(String, String)] = appDict.map(_.split("\t")).filter(_.length == 5).map(x => (x(4), x(1)))
    val appDf: DataFrame = ss.createDataFrame(appDictRDD)

    // 读取 parquet 格式文件
    val df: DataFrame = ss.read.parquet(inputPath)
    // 创建字典的临时表
    appDf.createOrReplaceTempView("appname_dict")
    // 创建数据清洗后的临时表
    df.createOrReplaceTempView("etled_data")
    // 创建媒体分析SQL语句 没有和字典进行join appname

    //    val appnameSql = "select appname," +
    //      "count(case when requestmode=1 and processnode>=1 then 1 else null end) as rawrequest," +
    //      "count(case when requestmode=1 and processnode>=2 then 1 else null end) as validrequest," +
    //      "count(case when requestmode=1 and processnode=3 then 1 else null end) as adrequest," +
    //      "count(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else null end) as joinbidding," +
    //      "count(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else null end) as succbidding," +
    //      "count(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else null end)/count(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else null end) as biddingsuccrate," +
    //      "count(case when requestmode=2 and  iseffective=1 then 1 else null end) as showcount," +
    //      "count(case when requestmode=3 and  iseffective=1 then 1 else null end) as clickcount," +
    //      "count(case when requestmode=3 and  iseffective=1 then 1 else null end)/count(case when requestmode=2 and  iseffective=1 then 1 else null end) as clickrate," +
    //      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0 end)/1000 as adcost," +
    //      "sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0 end)/1000 as adconsumer " +
    //      "from etled_data  group by appname"

    val appnameSql = "select if(appname='',_2,appname) as appname," +
      "rawrequest,validrequest,adrequest,joinbidding,succbidding,biddingsuccrate,showcount,clickcount,clickrate,adcost,adconsumer from " +
      "(select appid,appname,_2, " +
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
      "from etled_data as e left join appname_dict as a on e.appid=a._1 group by appname,appid,_2) "



    // 创建渠道报表分析SQL语句
    val adplatformprovideridSql = "select adplatformproviderid," +
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
      "from etled_data group by adplatformproviderid"

    // 创建配置文件
    val prop = new Properties()
    prop.put("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
    prop.put("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
    val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    // 写入mysql
    ss.sql(appnameSql).write.mode("overwrite").jdbc(url, "appname", prop)
    ss.sql(adplatformprovideridSql).write.mode("overwrite").jdbc(url, "adplatformproviderid", prop)
    ss.stop()
  }
}
