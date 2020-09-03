package com.atguigu.gmall0317.realtime.dws

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object DwsOrderWideWithCacheApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("dws_order_wide_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val groupId = "dws_order_wide_group"
    val topicOrderInfo = "DWD_ORDER_INFO"
    val topicOrderDetail = "DWD_ORDER_DETAIL"

    val offsetMapForKafkaOrderInfo =
      OffsetManager.getOffset(topicOrderInfo, groupId)
    val offsetMapForKafkaOrderDetail =
      OffsetManager.getOffset(topicOrderDetail, groupId)

    var recordInputDstreamOrderInfo:
      InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafkaOrderInfo!= null &&
      offsetMapForKafkaOrderInfo.size>0) {
      recordInputDstreamOrderInfo = MyKafkaUtil.
        getKafkaStream(topicOrderInfo, ssc, offsetMapForKafkaOrderInfo, groupId)
    } else {
      recordInputDstreamOrderInfo = MyKafkaUtil.getKafkaStream(topicOrderInfo, ssc, groupId)
    }

    var recordInputDstreamOrderDetail:
      InputDStream[ConsumerRecord[String, String]] = null
    if (offsetMapForKafkaOrderDetail!= null &&
      offsetMapForKafkaOrderDetail.size>0) {
      recordInputDstreamOrderDetail = MyKafkaUtil.
        getKafkaStream(topicOrderDetail, ssc, offsetMapForKafkaOrderDetail, groupId)
    } else {
      recordInputDstreamOrderDetail = MyKafkaUtil.
        getKafkaStream(topicOrderDetail, ssc, groupId)
    }



    var offsetRangesOrderDetail: Array[OffsetRange] = null
    val inputGetOffsetDstreamOrderDetail =
      recordInputDstreamOrderDetail.transform {
      rdd =>
        offsetRangesOrderDetail = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    var offsetRangesOrderInfo: Array[OffsetRange] = null
    val inputGetOffsetDstreamOrderInfo =
      recordInputDstreamOrderInfo.transform {
        rdd =>
          offsetRangesOrderInfo = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          rdd
      }


    val orderDetailDstream = inputGetOffsetDstreamOrderDetail.map {
      record =>
        val jsonString = record.value()
        val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
    }
    orderDetailDstream.print(100)

    val orderInfoDstream = inputGetOffsetDstreamOrderInfo.map {
      record =>
        val jsonString = record.value()
        val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        orderInfo
    }
    orderInfoDstream.print(100)


    val orderInfoWithKeyDstream =
      orderInfoDstream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailWithKeyDstream =
      orderDetailDstream.map(orderDetail => (orderDetail.order_id, orderDetail))

    val fullJoine
      dDstream =
      orderInfoWithKeyDstream.fullOuterJoin(orderDetailWithKeyDstream)

    val orderWideDstream = orderTupleDstream.map {
      case (orderId,(orderInfo: OrderInfo, orderDetail: OrderDetail)) =>
        new OrderWide(orderInfo, orderDetail)
    }

    val filterOrderWideDstream = orderWideDstream.mapPartitions {
      orderWideItr =>
        val jedis = RedisUtil.getJedisClient
        val orderWideList = orderWideItr.toList
        val filteredOrderWideList = new ListBuffer[OrderWide]
        if (orderWideList != null && orderWideList.size > 0) {
          val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          for (orderWide <- orderWideList) {
            val dt = orderWide.dt
            val key = "orderwide" + dt
            val ts = dateFormat.parse(orderWide.create_time).getTime
            val isNonExists = jedis.zadd(key, ts, orderWide.order_detail_id.toString)
            if (isNonExists == 1L) {
              filteredOrderWideList.append(orderWide)
            }
          }
        }
        jedis.close()
        filteredOrderWideList.toIterator
    }

    filterOrderWideDstream.print(1000)
    ssc.start()
    ssc.awaitTermination()

  }
}
