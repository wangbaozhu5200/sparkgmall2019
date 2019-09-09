package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DAUHandler {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
    * 不同批次去重
    *
    * @param startLogDStream
    */
  def filterDataByRedis(ssc: StreamingContext, startLogDStream: DStream[StartUpLog]) = {

    startLogDStream.transform(rdd => {

      println(s"第一次去重前：${rdd.count()}")
      //从redis获取mid
      val jedis: Jedis = RedisUtil.getJedisClient
      val date: String = sdf.format(new Date(System.currentTimeMillis()))
      val mids: util.Set[String] = jedis.smembers(s"dau:$date")
      val midsBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(mids)
      jedis.close()

      //去重
      val value: RDD[StartUpLog] = rdd.filter(mlog => {
        val midBCValue: util.Set[String] = midsBC.value
        !midBCValue.contains(mlog.mid)
      })

      println(s"第一次去重后：${value.count()}")
      value
    })
  }

  /**
    *
    * @param startLogDStream
    */
  def saveUserToRedis(startLogDStream: DStream[StartUpLog]) = {

    startLogDStream.foreachRDD(rdd => {
      rdd.foreachPartition(items => {
        //获取redis
        val jedis: Jedis = RedisUtil.getJedisClient

        items.foreach(startlog => {
          //rediskey
          val redisKey = s"dau:${startlog.logDate}"
          jedis.sadd(redisKey, startlog.mid)
        })

        //关闭redis
        jedis.close()

      })
    })
  }

}
