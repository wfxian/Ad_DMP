package com.util

import java.util

import redis.clients.jedis._

object JedisClusters {
  //连接Redis集群
  def JedisC : JedisCluster = {
    val config = new JedisPoolConfig
    // 最大连接数, 默认8个
    config.setMaxTotal(100)
    // 最大空闲连接数, 默认8个
    config.setMaxIdle(50)
    //最小空闲连接数, 默认0
    config.setMaxWaitMillis(10)

    //集群模式
    val nodes = new util.HashSet[HostAndPort]()

    val hostAndPort1 = new HostAndPort("redis",7001)
    val hostAndPort2 = new HostAndPort("redis",7002)
    val hostAndPort3 = new HostAndPort("redis",7003)

    nodes.add(hostAndPort1)
    nodes.add(hostAndPort2)
    nodes.add(hostAndPort3)

    val jedis = new JedisCluster(nodes,config)
    jedis
  }

  //连接Jedis单机
  def JedisD: Jedis ={
    val config = new JedisPoolConfig
    // 最大连接数, 默认8个
    config.setMaxTotal(100)
    // 最大空闲连接数, 默认8个
    config.setMaxIdle(50)
    //最小空闲连接数, 默认0
    config.setMaxWaitMillis(10)

    val pool = new JedisPool(config,"hadoop02",6379)
    val jedis: Jedis = pool.getResource
    jedis
  }



}
