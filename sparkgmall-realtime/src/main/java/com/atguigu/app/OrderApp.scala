package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")

    val ssc = new StreamingContext(conf, Seconds(5))
    val KafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_ORDER_INFO))
    //格式化数据
    val orderInfoDStream: DStream[OrderInfo] = KafkaDStream.map { case (_, value) =>
      //转化为orderinfo对象
      val orderinfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
      val tuple: (String, String) = orderinfo.consignee_tel.splitAt(4)
      orderinfo.consignee_tel = s"${tuple._1}*******"
      orderinfo.create_date = orderinfo.create_time.split(" ")(0)
      orderinfo.create_hour = orderinfo.create_time.split(" ")(1).split(":")(0)
      orderinfo
    }
    //写入Hbase
    orderInfoDStream.foreachRDD(rdd => {
      import org.apache.phoenix.spark._
      rdd.saveToPhoenix("GMALL_ORDER_INFO", Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"), new Configuration, Some("hadoop101,hadoop102,hadoop103:2181"))
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
