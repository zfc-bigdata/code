package com.atguigu.gmall0317.realtime.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0317.realtime.bean.OrderInfo
import com.atguigu.gmall0317.realtime.util.{MyEsUtil, MyKafkaSender, MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object DwdOrderInfo {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("dwd_order_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_T_ORDER_INFO"
    val groupId = "dwd_order_info_group"

    var inputDstream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMap = OffsetManager.getOffset(topic, groupId)

    if (offsetMap != null) {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    } else {
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val inputWithOffsetDstream = inputDstream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val orderInfoDstream = inputWithOffsetDstream.map {
      record =>
        val jsonString = record.value()
        val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        val createTimeArr = orderInfo.create_time.split(" ")
        orderInfo.create_date = createTimeArr(0)
        orderInfo.create_hour = createTimeArr(1).split(":")(0)
        orderInfo
    }

    val orderInfoWithProvinceDstream = orderInfoDstream.transform {
      rdd =>
        val provinceJsonObjList = PhoenixUtil
          .queryList("select * from gmall0317_province_info")
        val provinceInfoMap = provinceJsonObjList.map(jsonObject =>
          (jsonObject.getString("ID"), jsonObject)).toMap
        val provinceInfoMapBC = ssc.sparkContext.broadcast(provinceInfoMap)
        val orderInfoWithProvinceRdd = rdd.map {
          orderInfo =>
            val provinceMap = provinceInfoMapBC.value
            val provinceJsonObj = provinceMap.getOrElse(orderInfo.province_id.toString, null)
            if (provinceJsonObj != null) {
              orderInfo.province_3166_2_code =
                provinceJsonObj.getString("ISO_3166_2")
              orderInfo.province_name =
                provinceJsonObj.getString("NAME")
              orderInfo.province_area_code =
                provinceJsonObj.getString("AREA_CODE")
            }
            orderInfo
        }
        orderInfoWithProvinceRdd

    }
   /* val orderInfoWithProvinceRdd = orderInfoDstream.map {
      orderInfo =>
        val provinceJsonObjList =
          PhoenixUtil.queryList("select * from gmall0317_province_info where id='" + orderInfo.province_id + "'")
        val provinceJsonObj = provinceJsonObjList(0)
        orderInfo.province_3166_2_code = provinceJsonObj.getString("province_3166_2_code")
        orderInfo
    }*/

    /*val orderInfoWithProvinceDstream = orderInfoDstream.transform {
      rdd =>
        val provinceJsonObjList = PhoenixUtil.queryList("select * from gmall0317_province_info")
        val provinceInfoMap = provinceJsonObjList.map(
          jsonObj =>
            (jsonObj.getString("ID"), jsonObj)
        ).toMap
        val provinceInfoMapBC = ssc.sparkContext.broadcast(provinceInfoMap)
        val orderInfoWithProvinceRdd = rdd.map {
          orderInfo =>
            val provinceMap = provinceInfoMapBC.value
            val provinceJsonObj = provinceMap.getOrElse(orderInfo.province_id.toString, null)
            if (provinceJsonObj != null) {
              orderInfo.province_3166_2_code = provinceJsonObj.getString("ISO_3166_2")
              orderInfo.province_name = provinceJsonObj.getString("NAME")
              orderInfo.province_area_code = provinceJsonObj.getString("AREA_CODE")
            }
            orderInfo
        }*/
    orderInfoWithProvinceDstream.print(100)

    orderInfoWithProvinceDstream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          orderInfoItr =>
            val orderInfolist = orderInfoItr.toList
            if (orderInfolist!=null && orderInfolist.size>0) {
              val create_date = orderInfolist(0).create_date
              val orderInfoWithIdList =
                orderInfolist.map(
                  orderInfo => (orderInfo, orderInfo.id.toString))
              val indexName = "gmall0317_order_info" + create_date
              MyEsUtil.saveDocBulk(orderInfoWithIdList,indexName)
            }
            for (orderInfo <- orderInfolist){
              MyKafkaSender.send("DWD_ORDER_INFO",
                JSON.toJSONString(orderInfo,new SerializeConfig(true)))
            }
        }
      OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()

  }
}
