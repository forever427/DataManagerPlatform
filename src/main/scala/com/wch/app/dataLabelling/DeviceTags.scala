package com.wch.app.dataLabelling

import org.apache.spark.sql.Row

object DeviceTags extends Tags {
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()
    // 设备
    val row: Row = args(0).asInstanceOf[Row]
    // 设备操作系统
    val client: Int = row.getAs[Int]("client")
    var clientStr = client match {
      case 1 => list:+=("D00010001",1)
      case 2 => list:+=("D00010002",1)
      case 3 => list:+=("D00010003",1)
      case _ => list:+=("D00010004",1)
    }

    // 设备联网方式
    val networkmannerid: Int = row.getAs[Int]("networkmannerid")
    networkmannerid match {
      case 1 => list:+=("D00020001",1)
      case 2 => list:+=("D00020002",1)
      case 3 => list:+=("D00020003",1)
      case 4 => list:+=("D00020004",1)
      case _ => list:+=("D00020005",1)
    }

    // 设备运营商方式
    var ispname: String = row.getAs("ispname")
    ispname match {
      case "移动" => list:+=("D00030001",1)
      case "联通" => list:+=("D00030002",1)
      case "电信" => list:+=("D00030003",1)
      case _ => list:+=("D00030004",1)
    }

    list
  }

}
