package com.wch.app.dataLabelling

import com.wch.utils.{HbaseUtil, JedisUtils, SparkUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis

object DataLabel {
  def main(args: Array[String]): Unit = {
    // 判断输入输出路径
    if (args.length != 4) {
      println("输入输出路径异常, 程序退出!!!")
      sys.exit()
    }
    val Array(inputPath, outputPath, dictPath, stopwords) = args
    val ss: SparkSession = SparkUtils.getSession
    val sc: SparkContext = ss.sparkContext

    // 获取APPname 字典 并广播出去
    val dict: Map[String, String] = sc.textFile(dictPath).map(_.split("\t", -1)).filter(_.length == 5).map(x => (x(4), x(1))).collect().toMap
    val appnameBroadcast: Broadcast[Map[String, String]] = sc.broadcast(dict)

    // 获取停用词 并广播
    val stopwordsMap: Map[String, Int] = sc.textFile(stopwords).map((_, 0)).collect().toMap
    val stopwordsBroadcast: Broadcast[Map[String, Int]] = sc.broadcast(stopwordsMap)

    // 读取源文件
    val df: DataFrame = ss.read.parquet(inputPath)
    // 将全为空的user过过滤掉
    val result: RDD[(List[String], Row)] = df.filter(TagUtils.hasneedOneUserId).rdd.map(row => {
      // 将第一个不为空的字段作为userID
      val userid: List[String] = TagUtils.getRowAllUserId(row)

      (userid, row)
    })

    // 构建点的集合
    val vre: RDD[(Long, List[(String, Int)])] = result.flatMap(tup => {
      // 获取redis链接
      //val jedis: Jedis = JedisUtils.getConnection()
      // 获取一条数据
      val row: Row = tup._2
      // 广告标签
      val adTag: List[(String, Int)] = AdTags.makeTags(row)
      // appname
      val appTag: List[(String, Int)] = AppTags.makeTags(row, appnameBroadcast)
      // 设备类型
      val deviceTag: List[(String, Int)] = DeviceTags.makeTags(row)
      // 关键字
      val keywordsTag: List[(String, Int)] = KeywordsTags.makeTags(row, stopwordsBroadcast)
      // 商圈   由于数据的问题, 这里加入商圈标签会导致运行异常, 所以没有加入商圈标签
      //val businessTag: List[(String, Int)] = BusinessTags.makeTags(row, jedis)
      // 地域信息
      val localTag: List[(String, Int)] = LocationTags.makeTags(row)

      //val currentTag: List[(String, Int)] = adTag ++ appTag ++ deviceTag ++ keywordsTag ++ businessTag ++ localTag
      val currentTag: List[(String, Int)] = adTag ++ appTag ++ deviceTag ++ keywordsTag ++ localTag
      val vd: List[(String, Int)] = tup._1.map((_, 0)) ++ currentTag

      // 创建点的集合
      tup._1.map(uid => {
        if (tup._1.head.equals(uid)) {
          (uid.hashCode.toLong, vd)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })

    // (757068314,List((AOD:27fd0fde315c7f45,0), (IMM:03de29984baffb87a7338d0ead22c045,0), (LC12,1), (LN视频前贴片,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020003,1), (D00030004,1), (广东省,1), (茂名市,1)))
    // (437470833,List())
    // 测试打印前20个点
    // vre.take(20).foreach(println)

    // 构建边的集合
    val edges: RDD[Edge[Int]] = result.flatMap(tup => {

      tup._1.map(uid => {
        Edge(tup._1.head.hashCode.toLong, uid.hashCode.toLong, 0)
      })
    })
    // Edge(757068314,757068314,0)
    // Edge(757068314,437470833,0)
    // 测试打印前20个边
    // edges.take(20).foreach(println)

    val graph: Graph[List[(String, Int)], Int] = Graph(vre, edges)

    val cc: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // (-1877161653,-1877161653)
    //(-1431419297,-1431419297)
    // 测试打印前20个图
    // cc.take(20).foreach(println)
                                                                                                                                        
    val joined: RDD[(VertexId, List[(String, Int)])] = cc.join(vre).map {
      case (uid, (commonId, tagsAndUserid)) => (commonId, tagsAndUserid)
    }
    val res: RDD[(VertexId, List[(String, Int)])] = joined.reduceByKey {
      case (list1, list2) => (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }
    // (-1877161653,List((D00020003,1), (APP爱奇艺,1), (LN视频前贴片,1), (CN100018,1), (D00030004,1), (江苏省,1), (AOD:693a8a53e915a138,0), (徐州市,1), (D00010001,1), (LC12,1), (IMM:1d6fdd5a9a2997e77d985fff8c13f5b5,0)))
    // 测试打印前20个join后的数据
    // res.take(20).foreach(println)

    // 存放到HBASE中
     res.map{
      case (userid,userTages)=>{
        val tags: String = userTages.map(t=>t._1+":"+t._2).mkString(",")
        // (1083562376,List((IMM:566fd6763e27dc1c5a0c9ff17848fe4f,0), (LC12,1), (LN视频前贴片,1), (CN100018,1), (APP爱奇艺,1), (D00010001,1), (D00020003,1), (D00030004,1), (K每日焦点,1), (广西壮族自治区,1), (桂林市,1)))
        HbaseUtil.insertTable("dmp:label",userid.toString,"f1","userTags",tags)
      }
    }
  }
}
