package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.constant.GmallConstants
import com.atguigu.constant.GmallConstants._
import com.atguigu.log.StartUpLog
import com.atguigu.utils.MykafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeStartupApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("gmall2019")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    //val startupStream: InputDStream[ConsumerRecord[String, String]] = MykafkaUtil.getKafkaStream(ssc, GMALL_STARTUP)

    //       startupStream.map(_.value()).foreachRDD{ rdd=>
    //         println(rdd.collect().mkString("\n"))
    //      }

    //val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map { log =>
      // println(s"log = ${log}")
      //val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
      //startUpLog
    }
  }
