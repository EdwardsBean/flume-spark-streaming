package com.edwardsbean.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import redis.clients.jedis.Jedis

/**
 *
 * 实时统计每个App的下载量,总下载量
 *
 * redis中保存2个字典：
 * 1:用户下载记录，用于查看最近下载同一款App是否一个小时内
 * 2:App下载量
 */
object AppDownloadCount {
  val jedis = new Jedis("localhost")

  //123 熊猫看书 1410505200000
  //123 91desktop 1410505200000
  case class UserDownload(imei: String,appName: String,timestamp: String)

  def etl(flumeEvent:SparkFlumeEvent): Boolean = {
    val raw = new String(flumeEvent.event.getBody.array())
    val pairs = raw.split(" ")
    pairs.size == 3 && checkTime(pairs(0),pairs(1),pairs(2))
  }

  def checkTime(imei:String, currentTime:String, appName:String):Boolean = {
    //查询redis
    val lastDownTime = jedis.hget(imei,appName)
    if(lastDownTime != null) {
      val parseLastTime = TimeUnit.MILLISECONDS.toHours(java.lang.Long.parseLong(lastDownTime))
      val parseCurrentTime = TimeUnit.MILLISECONDS.toHours(java.lang.Long.parseLong(currentTime))
      if((parseCurrentTime - parseLastTime) > 0){
        //更新用户下载记录
//        jedis.hset(imei,appName,currentTime)
        true
      }else{
        //一个用户同一小时多次下载该软件
        false
      }
    } else true
  }

  def parseRawUserDownload(flumeEvent:SparkFlumeEvent): UserDownload = {
    val raw = new String(flumeEvent.event.getBody.array())
    val pairs = raw.split(" ")
    UserDownload(pairs(0),pairs(1),pairs(2))
  }


  def AppDownloadCountAndWindow(source: DStream[UserDownload],duration:Duration): DStream[(String, Int)] = {
    source.map(userDownload => (userDownload.appName,1))
      .reduceByKeyAndWindow(_+_,duration)
  }

  def AppDownloadCount(source: DStream[UserDownload]): DStream[(String, Int)] = {
    source.map(userDownload => (userDownload.appName,1))
      .reduceByKey(_+_)
  }

  def sinkToRedis(result:DStream[(String, Int)]) = {
    result.foreachRDD {
      rdd => rdd.foreach{x=>
        val (appName,downCount) = x
        //聚合成总下载量
        jedis.hincrBy(appName,"totalDownloadCount",downCount)
        //单位时间内的下载量
        jedis.hset(appName,"downloadCount",downCount + "")

//        //通知下载量已变更
//        jedis.publish("downloadChange",appName)
      }
    }
    //测试，打印计算结果
    result.print()
  }


  def updateAppDownload(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {
    val currentCount = newValues.sum
    val previousCount = runningCount.getOrElse(0)
    Some(currentCount + previousCount)
  }


  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[2]", "AppDownloadCount", Seconds(1))
    val source = FlumeUtils.createStream(ssc,"localhost",5555,StorageLevel.MEMORY_ONLY)
    val cleanSource = source.filter(etl).map(parseRawUserDownload)
    val downloadCount = AppDownloadCount(cleanSource)
    sinkToRedis(downloadCount)

//    val lastDown = appDownFiltered.updateStateByKey[Int](updateAppDownload)
    ssc.start()
    ssc.awaitTermination()
  }
}
