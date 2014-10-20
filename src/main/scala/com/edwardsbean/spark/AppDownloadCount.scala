package com.edwardsbean.spark

import java.util.concurrent.TimeUnit

import com.typesafe.config.ConfigFactory
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import redis.clients.jedis.{JedisPool, Jedis}

/**
 *
 * 实时统计每个App的下载量,总下载量
 *
 * spark中保存1个字典：用户下载记录，用于查看最近下载同一款App是否一个小时内
 * redis中保存1个字典：App下载量，聚合用
 */
object AppDownloadCount {

  //123 熊猫看书 1410505200000
  //123 91desktop 1410505200000
  case class UserDownload(imei: String, appName: String, timestamp: Long)


  def etl(flumeEvent: SparkFlumeEvent): Boolean = {
    val raw = new String(flumeEvent.event.getBody.array())
    val pairs = raw.split(" ")
    pairs.size == 3
  }

  def parseRawUserDownload(flumeEvent: SparkFlumeEvent): UserDownload = {
    val raw = new String(flumeEvent.event.getBody.array())
    val pairs = raw.split(" ")
    UserDownload(pairs(0), pairs(1), pairs(2).toLong)
  }


  //  def AppDownloadCountAndWindow(source: DStream[UserDownload],duration:Duration): DStream[(String, Int)] = {
  //    source.map(userDownload => (userDownload.appName,1))
  //      .reduceByKeyAndWindow(_+_,duration)
  //  }

  def AppDownloadCount(source: DStream[UserDownload], dict: DStream[(String, Long)]): DStream[(String, Int)] = {
    val downloads = source.map(userDownload => (combine(userDownload.imei, userDownload.appName), userDownload.timestamp))
    downloads.cogroup(dict).map { case (key: String, (download: Iterable[Long], history: Iterable[Long])) =>
      val currentTime = download.head
      val previousTime = history.head
      val outputKey = getAppName(key)
      val time = TimeUnit.MILLISECONDS.toHours(currentTime - previousTime)
      if (time < 1) (outputKey, 0) else (outputKey, 1)
    }.reduceByKey(_ + _)
  }

  def updateAppDownload(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val currentCount = newValues.sum
    val previousCount = runningCount.getOrElse(0)
    Some(currentCount + previousCount)
  }

  def updateDict(newValues: Seq[Long], runningCount: Option[Long]): Option[Long] = {
    val currentCount = newValues.max
    val previousCount = runningCount.getOrElse(0.toLong)
    if (previousCount < currentCount) Some(currentCount) else Some(previousCount)
  }

  def createRedisPool(host: String, port: Int, pwd: String): JedisPool = {
    val pc = new GenericObjectPoolConfig()
    pc.setMaxIdle(5)
    pc.setMaxTotal(5)
    new JedisPool(pc, host, port, 10000, pwd)
  }

  def combine(imei: String, appName: String): String = {
    imei + "," + appName
  }

  def getAppName(combine: String): String = {
    combine.split(",")(1)
  }

  def sinkToRedis(downloadCount: DStream[(String, Int)], pool: Broadcast[JedisPool]): Unit = {
    downloadCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val jedis = pool.value.getResource
        partitionOfRecords.foreach { case (appName: String, downCount: Int) =>
          //聚合成总下载量
          jedis.hincrBy(appName, "totalDownloadCount", downCount)
          //单位时间内的下载量
          jedis.hset(appName, "downloadCount", downCount + "")
        }
        pool.value.returnResource(jedis)
      })
    })
  }

  def main(args: Array[String]) {
    val config = ConfigFactory.load()
    val master = config.getString("spark.master.ip")
    val ip = config.getString("spark.listen.ip")
    val port = config.getInt("spark.listen.port")
    val redisIp = config.getString("spark.redis.ip")
    val redisPort = config.getInt("spark.redis.port")
    val redisPwd = config.getString("spark.redis.pwd")

    val ssc = new StreamingContext(master, "AppDownloadCount", Seconds(1))
    val source = FlumeUtils.createStream(ssc, ip, port, StorageLevel.MEMORY_ONLY)

    //使用线程池复用链接
    val pool = {
      val pool = createRedisPool(redisIp, redisPort, redisPwd)
      ssc.sparkContext.broadcast(pool)
    }

    //预处理
    val cleanSource = source.filter(etl).map(parseRawUserDownload).cache()

    /**
     * TODO
     * 如何清除过期字典
     */
    val dict = cleanSource.map { userDownload =>
      (combine(userDownload.imei, userDownload.appName), userDownload.timestamp)
    }
    //实时字典：用户最近一次下载记录作为字典
    val currentDict = dict.updateStateByKey(updateDict)

    //统计用户下载记录
    val downloadCount = AppDownloadCount(cleanSource, currentDict)

    //打印
    downloadCount.print()

    //输出到Redis
    sinkToRedis(downloadCount, pool)


    ssc.start()
    ssc.awaitTermination()
  }

}
