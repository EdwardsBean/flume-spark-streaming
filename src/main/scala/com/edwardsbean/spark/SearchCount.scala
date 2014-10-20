package com.edwardsbean.spark


import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import redis.clients.jedis.Jedis

/**
 * 实时统计搜索热词
 *
 * 1:实时Top 10热词
 * 2:实时增长最快
 * 3:实时增长
 *
 * Created by edwardsbean on 14-9-15.
 */
object SearchCount {

  val jedis = new Jedis("localhost")
  case class SearchRecord(imei:String, requestTime:String, searchWord:String)

  def main(args: Array[String]) {
    val ssc = new StreamingContext("local[2]", "SearchWordCount", Seconds(1))
    //监听数据
    val searchSource: DStream[SearchRecord] = FlumeUtils.createStream(ssc,"localhost",5555,StorageLevel.MEMORY_ONLY)
      //读取flume的数据
      .map(flumeEvent => new String(flumeEvent.event.getBody.array()).split(" "))
      //格式化成对象SearchRecord
      .map(row => SearchRecord(row(0),row(1),row(2)))

    //计算 => (搜索词,搜索量)
    val searchResult:DStream[(String,Int)] = searchSource.map(searchRecord => (searchRecord.searchWord,1)).reduceByKey(_+_)

    //输出到redis
    searchResult.foreachRDD(rdd => rdd.foreach { x =>
      val (searchWord, searchCount) = x
      jedis.zadd("hostSearch",searchCount,searchWord)
      jedis.zincrby("hotSearchTotal",searchCount,searchWord)
    })

//    searchResult.foreachRDD(rdd => rdd.map(x => (x._2,x._1)).)
  }
}
