package com.atguigu.gmall0317.realtime.dwd

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.atguigu.gmall0317.realtime.bean.OrderDetail
import com.atguigu.gmall0317.realtime.util.{MyKafkaSender, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

object DwdOrderDetailApp {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("dwd_order_detail_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_T_ORDER_DETAIL"
    val groupId = "dwd_order_detail_group"

    var recordInputDstream: InputDStream[ConsumerRecord[String, String]] = null
    val offsetMapForKafka = OffsetManager.getOffset(topic, groupId)

    if (offsetMapForKafka != null) {
      recordInputDstream = MyKafkaUtil.
        getKafkaStream(topic, ssc, offsetMapForKafka, groupId)
    } else {
      recordInputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges: Array[OffsetRange] = null
    val inputGetOffsetDstream = recordInputDstream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val orderDetailDstream = inputGetOffsetDstream.map {
      record =>
        val jsonString = record.value()
        val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
        orderDetail
    }
    orderDetailDstream.foreachRDD{
      rdd =>
        rdd.foreach{
          orderDetail=>
            MyKafkaSender.send("DWD_ORDER_DETAIL",
              JSON.toJSONString(orderDetail,new SerializeConfig(true)))

        }
        OffsetManager.saveOffset(topic,groupId,offsetRanges)
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
