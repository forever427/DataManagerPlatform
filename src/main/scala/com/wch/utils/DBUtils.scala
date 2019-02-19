package com.wch.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.wch.conf.ConfigurationManager
import com.wch.constant.Constants

import scala.collection.mutable.ListBuffer

object DBUtils {
  val username = ConfigurationManager.getProperty(Constants.JDBC_USER)
  val password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  val url = ConfigurationManager.getProperty(Constants.JDBC_URL)
  try {
    val driver = ConfigurationManager.getProperty(Constants.JDBD_DRIVER)
    Class.forName(driver)
  } catch {
    case _: Exception => println("加载驱动类异常")
  }

  // 创建数据库连接池
  private val dataSource = new ListBuffer[Connection]

  // 创建指定数据库连接, 并放入连接池中
  private val dataSourceSize: Integer = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE)
  for (i <- 1 to dataSourceSize) {
    try {
      val conn: Connection = DriverManager.getConnection(url, username, password)
      dataSource.append(conn)
    } catch {
      case _: Exception => println("创建连接池异常")
    }
  }

  // 获取数据库连接
  def getConnection(): Connection = {
    while (dataSource.size == 0) {
      Thread.sleep(10)
    }
    dataSource.remove(1)
  }

  // 执行增删改语句
  def executeUpdate(sql: String, params: Array[Any]) = {
    // 初始化值
    var rtn = 0 // 每条SQL语句影响的行数
    var conn: Connection = null
    var ps: PreparedStatement = null
    // 获取连接
    try {
      conn = getConnection()
      // 获取ps对象
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(sql)
      // 为SQL语句赋值
      if (params != null && params.length > 0) {
        for (i <- 0 until params.length) {
          ps.setObject(i + 1, params(i))
        }
      }
      // 执行操作
      rtn = ps.executeUpdate()
      // 提交
      conn.commit()
    } finally {
      if (conn != null) {
        // 将连接重新放回到连接池中
        dataSource.append(conn)
      }
    }
    rtn
  }

  // 批量执行增删改语句
  def exeuteBatch(sql: String, paramsList: Array[Array[Any]]) = {
    // 初始化值
    var rtn: Array[Int] = null
    var conn: Connection = null
    var ps: PreparedStatement = null

    try {
      // 获取连接
      conn = getConnection()
      // 设置不自动提交
      conn.setAutoCommit(false)
      // 获取ps 对象
      ps = conn.prepareStatement(sql)

      // 开始为SQL语句赋值
      if (paramsList != null && paramsList.size > 0) {
        for (params <- paramsList) {
          for (i <- 0 until params.length) {
            ps.setObject(i + 1, params(i))
          }
          ps.addBatch()
        }
      }
      rtn = ps.executeBatch()
      // 提交任务
      conn.commit()
    } finally {
      // 将连接放回到连接池中
      if (conn != null) {
        dataSource.append(conn)
      }
    }
    // 返回结果
    rtn
  }
}
