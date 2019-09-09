package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date
import com.alibaba.fastjson.JSON
import org.apache.hadoop.conf.Configuration
import com.atguigu.constant.GmallConstants
import com.atguigu.handler.DAUHandler
import com.atguigu.bean.StartUpLog
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startupStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_STARTUP))

    val startLogDStream: DStream[StartUpLog] = startupStream.map {
      case (_, value) =>
        val startLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        //取出时间戳
        val ts: Long = startLog.ts
        val dateHour: String = sdf.format(new Date(ts))

        //给对象赋值，日期和小时
        val dateHourArr: Array[String] = dateHour.split(" ")
        startLog.logDate = dateHourArr(0)
        startLog.logHour = dateHour.split(" ")(1)

        //返回
        startLog
    }

    //redis去重（不同批次）
    val filterStartUplogDStream: DStream[StartUpLog] = DAUHandler.filterDataByRedis(ssc, startLogDStream)

    //redis去重（同批次）
    val distincStartUplogDStream: DStream[StartUpLog] = filterStartUplogDStream.map(log => (log.mid, log)).groupByKey().flatMap {
      case (mid, logiter) =>
        logiter.toList.take(1)
    }

    distincStartUplogDStream.cache()

    distincStartUplogDStream.foreachRDD(rdd => {
      println(s"第二次去重后：${rdd.count()}")
    })

    //将去重的写入Redis
    DAUHandler.saveUserToRedis(distincStartUplogDStream)

    //写入Hbase
        distincStartUplogDStream.foreachRDD(rdd => {
          import org.apache.phoenix.spark._
          rdd.saveToPhoenix("GMALL_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"), new Configuration, Some("hadoop101,hadoop102,hadoop103:2181"))

        })

    ssc.start()
    ssc.awaitTermination()
  }
}
