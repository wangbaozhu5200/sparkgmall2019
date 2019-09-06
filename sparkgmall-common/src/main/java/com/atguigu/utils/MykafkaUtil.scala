package com.atguigu.utils

import java.util.Properties

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

object MykafkaUtil {
//  def getKafkaStream(ssc: StreamingContext, topics: Set[String]): InputDStream[(String, String)] = {
//
//    val properties: Properties = PropertiesUtil.load("config.properties")
//    val kafkapara = Map(
//      "bootstrap.servers" -> properties.getProperty("kafka.broker.list"),
//      "group.id" -> "bigdata04"
//    )
//    //    val kafkaDStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream(ssc, kafkapara, topics)
//
//    //返回
//    //    kafkaDStream
//  }
}
