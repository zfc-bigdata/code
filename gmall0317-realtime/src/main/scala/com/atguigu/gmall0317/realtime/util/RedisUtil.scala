package com.atguigu.gmall0317.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient:Jedis ={
    if (jedisPool == null) {
      val config = PropertiesUtil.load("config.properties")
      val host = config.getProperty("redis.host")
      val port = config.getProperty("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(500)
      jedisPoolConfig.setTestOnBorrow(true)

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)

    }
    jedisPool.getResource
  }


}
