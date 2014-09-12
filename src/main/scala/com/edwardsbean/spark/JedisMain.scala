package com.edwardsbean.spark

import redis.clients.jedis.Jedis

/**
 * Created by edwardsbean on 14-9-12.
 */
object JedisMain {
  def main(args: Array[String]) {
    val jedis = new Jedis("localhost")
//    jedis.hset("123123","91助手","1000")
    println(jedis.hget("123123","91助手"))
  }
}
