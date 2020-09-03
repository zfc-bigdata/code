package com.atguigu.gmall0317.realtime.dim

import com.alibaba.fastjson.JSON
import com.atguigu.gmall0317.realtime.bean.ProvinceInfo
import com.atguigu.gmall0317.realtime.util.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.phoenix.spark._

object DimProvinceInfoApp {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("dim_province_info_app").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val topic = "ODS_T_BASE_PROVINCE"
    val groupId = "dim_province_info_group"

    var inputDstream : InputDStream[ConsumerRecord[String,String]] =null
    val offsetMap = OffsetManager.getOffset(topic, groupId)

    if (offsetMap!=null) {
      inputDstream = MyKafkaUtil
        .getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      inputDstream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    var offsetRanges:Array[OffsetRange] =null
    val inputWithOffsetDstream = inputDstream.transform {
      rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }

    val jsonObjDstream = inputWithOffsetDstream.map {
      record =>
        val jsonString = record.value()
        JSON.parseObject(jsonString)
    }

    jsonObjDstream.foreachRDD{
      rdd=>
        val provinceRDD = rdd.map {
          jsonObj =>
            val provinceInfo = ProvinceInfo(jsonObj.getString("id"),
              jsonObj.getString("name"),
              jsonObj.getString("area_code"),
              jsonObj.getString("iso_code"),
              jsonObj.getString("iso_3166_2")
            )
            println(provinceInfo)
            provinceInfo
        }
        provinceRDD.saveToPhoenix("GMALL0317_PROVINCE_INFO",
          Seq("ID","NAME","AREA_CODE","ISO_CODE","ISO_3166_2"),
          new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))

        OffsetManager.saveOffset(topic,groupId,offsetRanges)

    }

    ssc.start()
    ssc.awaitTermination()

  }


}
