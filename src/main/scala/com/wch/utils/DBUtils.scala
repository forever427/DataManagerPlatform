package com.wch.utils

import java.sql.{Connection, DriverManager}


object DBUtils {
  val IP = "localhost"
  val Port = "3306"
  val DBType = "mysql"
  val DBName = "dmp"
  val username = "root"
  val password = "root"
  val url = "jdbc:" + DBType + "://" + IP + ":" + Port + "/" + DBName

  classOf[com.mysql.jdbc.Driver]

  def getConnection(): Connection = {
    val conn: Connection = DriverManager.getConnection(url, username, password)
    conn
  }

  def close(conn: Connection): Unit = {
    try {
      if (!conn.isClosed() || conn != null) {
        conn.close()
      }
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
  }
}
