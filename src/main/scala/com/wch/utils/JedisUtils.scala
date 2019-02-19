package com.wch.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisUtils {
  // 创建redis配置文件
  val jedisPoolConfig = new JedisPoolConfig
  // 设置最大连接数
  jedisPoolConfig.setMaxTotal(20)
  // 设置最大空闲数
  jedisPoolConfig.setMaxIdle(10)
  // 当调用borrow object 进行有效性检查
  jedisPoolConfig.setTestOnBorrow(true)

  // 创建连接池  timeout 超时时间
  val pool = new JedisPool(jedisPoolConfig, "self", 7001, 10000)

  // 获取连接
  def getConnection() = {
    pool.getResource
  }
}
