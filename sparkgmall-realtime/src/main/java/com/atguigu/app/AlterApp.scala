package com.atguigu.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import com.alibaba.fastjson.JSON
import com.atguigu.bean.{AlertInfo, EventInfo}
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.util.control.Breaks._

object AlterApp {
  def main(args: Array[String]): Unit = {

    val sdf = new SimpleDateFormat("yyy-MM-dd HH")
    //创建SparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlterApp")

    val ssc = new StreamingContext(conf, Seconds(5))
    val KafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, Set(GmallConstants.GMALL_EVENTUP))
    //格式化数据
    val midToEventDStream: DStream[(String, EventInfo)] = KafkaDStream.map { case (key, value) =>
      val eventInfo: EventInfo = JSON.parseObject(value, classOf[EventInfo])
      val tims: Long = eventInfo.ts
      val dateArr: Array[String] = sdf.format(new Date(tims)).split(" ")
      eventInfo.logDate = dateArr(0)
      eventInfo.logHour = dateArr(1)

      (eventInfo.mid, eventInfo)
    }
    //开窗
    val minToEventIterDStream: DStream[(String, Iterable[EventInfo])] = midToEventDStream.window(Seconds(30)).groupByKey()

    //过滤数据
    val flagToAlertDStream: DStream[(Boolean, AlertInfo)] = minToEventIterDStream.map {
      case (mid, evetInfoItre) =>
        val uids = new util.HashSet[String]()
        val items = new util.HashSet[String]()
        val events = new util.ArrayList[String]()
        //定义一个变量
        var flag = true
        breakable {
          evetInfoItre.foreach(evetInfoItre => {

            events.add(evetInfoItre.evid) //记录用户行为

            //判断当前是否为领取优惠券行为
            if ("coupon".equals(evetInfoItre.`evid`)) {
              uids.add(evetInfoItre.uid) //记录用户数量
              items.add(evetInfoItre.itemid)
            } else if ("clickItem".equals(evetInfoItre.`evid`)) {
              flag = flag
              break()
            }
          })
        }
        (flag && uids.size() >= 3, AlertInfo(mid, uids, items, events, System.currentTimeMillis()))
    }
    //过滤数据
    val alertInfoSDtream: DStream[AlertInfo] = flagToAlertDStream.filter(_._1).map(_._2)
    //测试数据
    flagToAlertDStream.print()

    //将数据写入ES

    ssc.start()
    ssc.awaitTermination()

  }
}
